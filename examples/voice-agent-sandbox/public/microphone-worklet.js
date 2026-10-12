'use strict';
// Capture and resample away from the UI thread. Forty-ms packets keep browser
// transport under the demo's message limit while reducing 128-ms capture jitter.
class GiggyMicrophoneProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.packet = new Float32Array(640);
    this.position = 0;
    this.phase = 0;
    this.sum = 0;
    this.count = 0;
  }
  process(inputs) {
    const input = inputs[0]?.[0];
    if (!input) return true;
    for (const sample of input) {
      this.sum += sample; this.count++;
      this.phase += 16000;
      if (this.phase >= sampleRate) {
        this.phase -= sampleRate;
        this.packet[this.position++] = this.sum / this.count;
        this.sum = 0; this.count = 0;
        if (this.position === this.packet.length) {
          this.port.postMessage(this.packet, [this.packet.buffer]);
          this.packet = new Float32Array(640); this.position = 0;
        }
      }
    }
    return true;
  }
}
registerProcessor('giggy-microphone', GiggyMicrophoneProcessor);
