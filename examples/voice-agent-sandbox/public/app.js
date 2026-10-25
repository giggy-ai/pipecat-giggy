'use strict';
const $ = id => document.getElementById(id);
let socket, context, stream, processor, micSource, silentGain, analyser;
let active = false, history = [], capture = null, turnId = null, assistantBubble = null, userBubble = null;
let audioAt = 0, audioSources = new Set(), timer, deadline, assistantText = '';
let preRoll = [], onsetMs = 0;
let introPending = false;
function status(text) { $('session-status').textContent = text; }
function send(message) { if (socket?.readyState === WebSocket.OPEN) socket.send(JSON.stringify(message)); }
function voice() { return { voice_source: 'voice_library', voice_library_uuid: $('voice').value }; }
function bubble(role, text) {
  const div = document.createElement('div'); div.className = `bubble ${role}`;
  const label = document.createElement('span'); label.textContent = role === 'user' ? 'You' : 'Giggy assistant';
  const p = document.createElement('p'); p.textContent = text; div.append(label, p); $('chat').append(div);
  $('chat').scrollTop = $('chat').scrollHeight; return p;
}
function scrollChat() { $('chat').scrollTop = $('chat').scrollHeight; }
function clearAudio() { for (const source of audioSources) { try { source.stop(); } catch {} } audioSources.clear(); audioAt = 0; }
function interrupt() {
  if (turnId) send({ type: 'voice_agent_interrupt', turn_id: turnId });
  clearAudio(); turnId = null; assistantBubble = null; assistantText = '';
}
function newTurn() { interrupt(); turnId = crypto.randomUUID(); userBubble = null; return turnId; }
function pcmBase64(samples) {
  const bytes = new Uint8Array(samples.length * 2), view = new DataView(bytes.buffer);
  for (let i = 0; i < samples.length; i++) view.setInt16(i * 2, Math.round(Math.max(-1, Math.min(1, samples[i])) * 32767), true);
  let binary = ''; for (const byte of bytes) binary += String.fromCharCode(byte); return btoa(binary);
}
function resample(input, rate) {
  if (rate === 16000) return input;
  const ratio = rate / 16000, result = new Float32Array(Math.floor(input.length / ratio));
  for (let i = 0; i < result.length; i++) {
    const position = i * ratio, left = Math.floor(position), fraction = position - left;
    result[i] = input[left] * (1 - fraction) + (input[Math.min(left + 1, input.length - 1)] * fraction);
  }
  return result;
}
function microphoneSamples(samples) {
  if (!active || socket?.readyState !== WebSocket.OPEN) return;
  const rms = Math.sqrt(samples.reduce((sum, n) => sum + n * n, 0) / samples.length), now = performance.now();
  const frameMs = samples.length / 16000 * 1000;
  if (!capture) {
    onsetMs = rms >= 0.012 ? onsetMs + frameMs : 0;
    if (onsetMs < 80) {
      preRoll.push(samples.slice());
      while (preRoll.reduce((sum, frame) => sum + frame.length, 0) > 2560) preRoll.shift();
      return;
    }
    capture = { id: newTurn(), sequence: 0, started: now, lastSpeech: now };
    send({ type: 'voice_agent_audio_start', turn_id: capture.id, voice: voice(), thread_history: history.slice(-32) });
    for (const frame of preRoll) send({ type: 'voice_agent_audio_chunk', turn_id: capture.id,
      sequence: capture.sequence++, sample_rate: 16000, chunk_base64: pcmBase64(frame) });
    preRoll = []; onsetMs = 0;
    status('Listening…');
  }
  if (rms >= 0.007) capture.lastSpeech = now;
  send({ type: 'voice_agent_audio_chunk', turn_id: capture.id, sequence: capture.sequence++, sample_rate: 16000,
    chunk_base64: pcmBase64(samples) });
  if (now - capture.lastSpeech >= Number($('timing').value) || now - capture.started >= 28000) {
    send({ type: 'voice_agent_audio_end', turn_id: capture.id, voice: voice(), thread_history: history.slice(-32) });
    capture = null; preRoll = []; onsetMs = 0; status('Thinking…');
  }
}
function microphoneFrame(event) {
  microphoneSamples(resample(event.inputBuffer.getChannelData(0), context.sampleRate));
}
function playPcm(message) {
  if (!context || message.turn_id !== turnId) return;
  const binary = atob(message.chunk_base64), bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  const view = new DataView(bytes.buffer), buffer = context.createBuffer(1, bytes.length / 2, 24000);
  const samples = buffer.getChannelData(0);
  for (let i = 0; i < samples.length; i++) samples[i] = view.getInt16(i * 2, true) / 32768;
  const source = context.createBufferSource(); source.buffer = buffer; source.connect(analyser);
  audioAt = Math.max(context.currentTime + 0.04, audioAt); source.start(audioAt); audioAt += buffer.duration;
  audioSources.add(source); source.onended = () => { audioSources.delete(source); if (!audioSources.size && active && !capture) status('Listening…'); };
  status('Speaking…');
}
function onMessage(event) {
  let message; try { message = JSON.parse(event.data); } catch { return; }
  if (message.type === 'voice_agent_intro') {
    if (!active || !introPending) return;
    introPending = false;
    // User speech wins if it began before the introductory event arrived.
    if (capture || turnId) return;
    turnId = message.turn_id; assistantText = message.text;
    assistantBubble = bubble('assistant', message.text);
    status('Preparing intro…'); return;
  }
  if (message.turn_id && message.turn_id !== turnId) return;
  if (message.type === 'voice_agent_session_ready') status(introPending ? 'Preparing intro…' : 'Listening…');
  if (message.type === 'voice_agent_transcript_partial' || message.type === 'voice_agent_transcript_final') {
    if (!userBubble) userBubble = bubble('user', ''); userBubble.textContent = message.text;
    if (message.type === 'voice_agent_transcript_final') history.push({ role: 'user', content: message.text });
  }
  if (message.type === 'voice_agent_assistant_text_delta') {
    if (!assistantBubble) assistantBubble = bubble('assistant', '');
    assistantText += message.delta; assistantBubble.textContent = assistantText;
  }
  if (message.type === 'voice_agent_audio_chunk') playPcm(message);
  if (message.type === 'voice_agent_turn_done') {
    history.push({ role: 'assistant', content: message.text }); history = history.slice(-32);
    if (!audioSources.size) status('Listening…');
  }
  if (message.type === 'voice_agent_error') {
    capture = null; clearAudio(); introPending = false;
    status(message.intro ? 'Intro unavailable. You can still speak or type.' : message.message);
  }
  scrollChat();
}
async function stop(text = 'Conversation stopped.') {
  active = false; introPending = false; send({ type: 'voice_agent_session_end' }); socket?.close(); socket = null;
  clearAudio(); capture = null; turnId = null; preRoll = []; onsetMs = 0; clearInterval(timer); $('timer').textContent = '';
  processor?.disconnect(); micSource?.disconnect(); silentGain?.disconnect();
  if (processor) processor.onaudioprocess = null;
  if (processor?.port) { processor.port.onmessage = null; processor.port.close(); }
  stream?.getTracks().forEach(track => track.stop()); stream = null;
  const previousContext = context; context = null; analyser = null;
  await previousContext?.close().catch(() => {});
  $('start').hidden = false; $('stop').hidden = true; $('voice').disabled = false;
  $('message').disabled = true; $('send').disabled = true; status(text);
}
async function start() {
  $('start').disabled = true; status('Preparing microphone…');
  try {
    context = new AudioContext(); await context.resume();
    stream = await navigator.mediaDevices.getUserMedia({ audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true } });
    analyser = context.createAnalyser(); analyser.fftSize = 256; analyser.connect(context.destination);
    socket = new WebSocket(`${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/api/voice-agent/realtime`);
    socket.onmessage = onMessage;
    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => { socket?.close(); reject(new Error('Voice connection timed out.')); }, 10000);
      socket.onopen = () => { clearTimeout(timeout); resolve(); };
      socket.onerror = () => { clearTimeout(timeout); reject(new Error('Connect your API keys again to start.')); };
    });
    active = true; introPending = true; socket.onclose = () => { if (active) void stop('Session ended. Reconnect your keys if they expired.'); };
    send({ type: 'voice_agent_session_start', voice: voice(), agent_instructions: 'Be friendly, concise, and conversational.' });
    micSource = context.createMediaStreamSource(stream);
    if (context.audioWorklet && typeof AudioWorkletNode !== 'undefined') {
      await context.audioWorklet.addModule('/microphone-worklet.js');
      processor = new AudioWorkletNode(context, 'giggy-microphone');
      processor.port.onmessage = event => microphoneSamples(event.data);
    } else {
      // Compatibility fallback for browsers without AudioWorklet.
      processor = context.createScriptProcessor(2048, 1, 1);
      processor.onaudioprocess = microphoneFrame;
    }
    silentGain = context.createGain(); silentGain.gain.value = 0;
    micSource.connect(processor); processor.connect(silentGain); silentGain.connect(context.destination);
    $('start').hidden = true; $('stop').hidden = false; $('voice').disabled = true;
    $('message').disabled = false; $('send').disabled = false;
    deadline = Date.now() + 15 * 60000;
    timer = setInterval(() => {
      const seconds = Math.max(0, Math.ceil((deadline - Date.now()) / 1000));
      $('timer').textContent = `${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, '0')}`;
      if (!seconds) void stop('15-minute conversation finished.');
    }, 1000);
  } catch (error) { await stop(error.message || 'Microphone unavailable. Check your browser permissions.'); }
  finally { $('start').disabled = false; }
}
function showDemo(voices) {
  $('voice').replaceChildren(...voices.map(v => { const option = document.createElement('option'); option.value = v.id; option.textContent = v.name; return option; }));
  const defaultVoice = voices.find(v => v.id === '93d2b122-9dc4-53a6-8ad9-44ebd14e229d');
  if (defaultVoice) $('voice').value = defaultVoice.id;
  $('setup').hidden = true; $('demo').hidden = false;
  $('demo-heading').hidden = false; $('disconnect').hidden = false;
}
$('connect-form').onsubmit = async event => {
  event.preventDefault(); $('connect').disabled = true; $('setup-status').textContent = 'Validating your accounts…';
  try {
    const response = await fetch('/api/session', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ giggyKey: $('giggy-key').value, deepseekKey: $('deepseek-key').value }) });
    const data = await response.json(); if (!response.ok) throw new Error(data.error);
    $('giggy-key').value = ''; $('deepseek-key').value = ''; $('setup-status').textContent = ''; showDemo(data.voices);
  } catch (error) { $('setup-status').textContent = error.message || 'Could not connect.'; }
  finally { $('connect').disabled = false; }
};
$('start').onclick = start; $('stop').onclick = () => void stop();
$('disconnect').onclick = async () => {
  await stop(); await fetch('/api/session', { method: 'DELETE' });
  history = []; $('chat').replaceChildren(); $('demo').hidden = true; $('setup').hidden = false;
  $('demo-heading').hidden = true; $('disconnect').hidden = true;
};
$('message-form').onsubmit = event => {
  event.preventDefault(); const text = $('message').value.trim(); if (!active || !text) return;
  if (capture) { send({ type: 'voice_agent_interrupt', turn_id: capture.id }); capture = null; }
  const id = newTurn(); bubble('user', text);
  send({ type: 'voice_agent_user_text', turn_id: id, text, voice: voice(), thread_history: history.slice(-32) });
  history.push({ role: 'user', content: text }); $('message').value = ''; status('Thinking…');
};
const canvas = $('visualizer'), draw = canvas.getContext('2d'), levels = new Uint8Array(128);
function animate() {
  draw.clearRect(0, 0, 600, 600); if (analyser) analyser.getByteFrequencyData(levels); else levels.fill(0);
  for (let i = 0; i < 96; i++) {
    const angle = i / 96 * Math.PI * 2, radius = 218, length = 4 + (levels[i] || 0) / 255 * 55;
    draw.beginPath(); draw.strokeStyle = `hsl(${185 + i / 96 * 170},85%,68%)`; draw.lineWidth = 3;
    draw.moveTo(300 + Math.cos(angle) * radius, 300 + Math.sin(angle) * radius);
    draw.lineTo(300 + Math.cos(angle) * (radius + length), 300 + Math.sin(angle) * (radius + length)); draw.stroke();
  }
  requestAnimationFrame(animate);
}
animate();
fetch('/api/session').then(async response => { if (response.ok) showDemo((await response.json()).voices); }).catch(() => {});
window.addEventListener('pagehide', () => { stream?.getTracks().forEach(track => track.stop()); socket?.close(); });
