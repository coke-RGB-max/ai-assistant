/**
 * AIRI 全双工语音通话模块 v1.0
 *
 * 功能：
 * - AudioWorklet 16kHz 音频采集（重采样 + 帧累积）
 * - VAD 语音活动检测（基于音量阈值 + 静音超时，轻量版）
 * - WebSocket 双向音频流（用户音频上行 + AI 音频下行）
 * - 流式音频播放（接收到 PCM 分片立即播放，低延迟）
 * - 打断机制（用户说话时自动中断 AI TTS）
 * - 回声抑制（AI 说话时降低麦克风增益，防止自激）
 * - Live2D 状态联动（聆听中/思考中/说话中）
 * - 通话结束后返回文字记录
 *
 * 用法：
 *   const call = new VoiceCallModule({
 *     wsUrl: "ws://localhost:8004/ws/call",
 *     roleId: "nianqi",
 *     userId: "user123",
 *     sessionId: null,
 *     onStateChange: (state) => {...},
 *     onAsrPartial: (text) => {...},
 *     onAsrFinal: (text) => {...},
 *     onAiReply: (text) => {...},
 *     onCallEnded: (transcript) => {...},
 *     onError: (msg) => {...},
 *   });
 *   await call.start();
 *   // ... 通话中 ...
 *   await call.hangup();
 */

class VoiceCallModule {
  constructor(options = {}) {
    // 配置
    this.wsUrl = options.wsUrl || "ws://localhost:8004/ws/call";
    this.roleId = options.roleId || "nianqi";
    this.userId = options.userId || "guest";
    this.sessionId = options.sessionId || null;

    // 回调
    this.onStateChange = options.onStateChange || (() => {});
    this.onAsrPartial = options.onAsrPartial || (() => {});
    this.onAsrFinal = options.onAsrFinal || (() => {});
    this.onAiReply = options.onAiReply || (() => {});
    this.onCallEnded = options.onCallEnded || (() => {});
    this.onError = options.onError || (() => {});
    this.onVolume = options.onVolume || (() => {});

    // 状态
    this.state = "idle"; // idle | connecting | listening | thinking | speaking | disconnected
    this.ws = null;
    this.wsConnected = false;

    // 音频采集
    this.mediaStream = null;
    this.audioContext = null;
    this.sourceNode = null;
    this.workletNode = null;
    this.micGainNode = null; // 麦克风增益控制（回声抑制用）

    // 音频播放
    this.playContext = null;
    this.playGainNode = null;
    this.audioQueue = [];
    this.isPlaying = false;
    this.currentSource = null;

    // VAD
    this.vadEnabled = true;
    this.vadThreshold = 0.015; // 音量阈值
    this.vadSilenceTimeout = 800; // 静音超时（毫秒）
    this.vadMinSpeechDuration = 300; // 最短语音时长
    this.isSpeech = false;
    this.speechStartTime = 0;
    this.silenceStartTime = 0;
    this.speechBuffer = [];

    // 回声抑制
    this.echoSuppressionEnabled = true;
    this.echoSuppressionGain = 0.1; // AI 说话时麦克风增益降到 10%
    this.normalMicGain = 1.0;

    // 通话记录
    this.transcript = [];

    // 控制
    this._interrupted = false;
    this._closed = false;
    this._workletRegistered = false;
  }

  // ============================================================
  // 通话控制
  // ============================================================

  async start() {
    if (this.state !== "idle" && this.state !== "disconnected") {
      console.warn("[VoiceCall] 已经在通话中");
      return false;
    }

    try {
      this._setState("connecting");
      this._closed = false;
      this.transcript = [];

      // 1. 获取麦克风权限
      await this._initAudioCapture();

      // 2. 连接 WebSocket
      await this._connectWebSocket();

      // 3. 发送 start 消息
      this.ws.send(JSON.stringify({
        action: "start",
        role_id: this.roleId,
        user_id: this.userId,
        session_id: this.sessionId,
      }));

      console.log("[VoiceCall] 通话已启动");
      return true;
    } catch (err) {
      console.error("[VoiceCall] 启动失败:", err);
      this.onError(`启动失败: ${err.message || err}`);
      this._cleanup();
      this._setState("disconnected");
      return false;
    }
  }

  async hangup() {
    if (this._closed) return;
    this._closed = true;

    console.log("[VoiceCall] 挂断通话");

    // 发送挂断消息
    if (this.ws && this.wsConnected) {
      try {
        this.ws.send(JSON.stringify({ action: "hangup" }));
      } catch (e) {
        // 忽略
      }
    }

    // 等待一小段时间接收 call_ended 消息
    await new Promise(r => setTimeout(r, 300));

    this._cleanup();
    this._setState("disconnected");
  }

  async interrupt() {
    // 手动打断 AI 说话
    if (this.state === "speaking" && this.ws && this.wsConnected) {
      this.ws.send(JSON.stringify({ action: "interrupt" }));
      this._stopPlayback();
      this._setState("listening");
    }
  }

  // ============================================================
  // 音频采集（AudioWorklet 16kHz）
  // ============================================================

  async _initAudioCapture() {
    // 获取麦克风
    this.mediaStream = await navigator.mediaDevices.getUserMedia({
      audio: {
        echoCancellation: true,
        noiseSuppression: true,
        autoGainControl: true,
        channelCount: 1,
        sampleRate: 48000,
      },
    });

    // 创建 AudioContext
    this.audioContext = new (window.AudioContext || window.webkitAudioContext)({
      sampleRate: 48000,
    });

    // 麦克风源
    this.sourceNode = this.audioContext.createMediaStreamSource(this.mediaStream);

    // 麦克风增益节点（用于回声抑制）
    this.micGainNode = this.audioContext.createGain();
    this.micGainNode.gain.value = this.normalMicGain;

    // 注册 AudioWorklet
    if (!this._workletRegistered) {
      const workletCode = this._getWorkletCode();
      const blob = new Blob([workletCode], { type: "application/javascript" });
      const workletUrl = URL.createObjectURL(blob);
      await this.audioContext.audioWorklet.addModule(workletUrl);
      URL.revokeObjectURL(workletUrl);
      this._workletRegistered = true;
    }

    // 创建 Worklet 节点
    this.workletNode = new AudioWorkletNode(
      this.audioContext,
      "audio-capture-worklet",
      { numberOfInputs: 1, numberOfOutputs: 1 }
    );

    // 接收音频帧
    this.workletNode.port.onmessage = (event) => {
      if (this._closed) return;
      const { buffer, volume } = event.data;
      this._handleAudioFrame(buffer, volume);
    };

    // 连接：源 → 增益 → Worklet
    this.sourceNode.connect(this.micGainNode);
    this.micGainNode.connect(this.workletNode);

    console.log("[VoiceCall] 音频采集已启动");
  }

  _getWorkletCode() {
    // AudioWorkletProcessor：48kHz → 16kHz 重采样 + 帧累积 + 音量计算
    return `
      class AudioCaptureWorklet extends AudioWorkletProcessor {
        constructor() {
          super();
          this.targetSampleRate = 16000;
          this.sourceSampleRate = sampleRate;
          this.ratio = this.sourceSampleRate / this.targetSampleRate;
          this.frameSize = 512; // 16kHz 下的帧大小
          this.buffer = new Float32Array(this.frameSize);
          this.bufferPos = 0;
          this.accumulator = 0;
          this.volumeSum = 0;
          this.volumeCount = 0;
        }

        process(inputs, outputs, parameters) {
          const input = inputs[0];
          if (!input || input.length === 0) return true;
          const channel = input[0];
          if (!channel) return true;

          for (let i = 0; i < channel.length; i++) {
            this.accumulator += 1;
            if (this.accumulator >= this.ratio) {
              this.accumulator -= this.ratio;
              const sample = channel[i];
              this.buffer[this.bufferPos++] = sample;
              this.volumeSum += Math.abs(sample);
              this.volumeCount++;

              if (this.bufferPos >= this.frameSize) {
                const avgVolume = this.volumeCount > 0 ? this.volumeSum / this.volumeCount : 0;
                this.port.postMessage({
                  buffer: this.buffer.slice(),
                  volume: avgVolume,
                });
                this.bufferPos = 0;
                this.volumeSum = 0;
                this.volumeCount = 0;
              }
            }
          }
          return true;
        }
      }
      registerProcessor('audio-capture-worklet', AudioCaptureWorklet);
    `;
  }

  // ============================================================
  // VAD 语音活动检测
  // ============================================================

  _handleAudioFrame(buffer, volume) {
    // 音量回调（用于 UI 波形显示）
    this.onVolume(volume);

    // 回声抑制：AI 说话时降低麦克风增益
    if (this.echoSuppressionEnabled && this.state === "speaking") {
      if (this.micGainNode) {
        this.micGainNode.gain.setTargetAtTime(
          this.echoSuppressionGain,
          this.audioContext.currentTime,
          0.05
        );
      }
    } else {
      if (this.micGainNode) {
        this.micGainNode.gain.setTargetAtTime(
          this.normalMicGain,
          this.audioContext.currentTime,
          0.05
        );
      }
    }

    // 发送音频到后端（无论是否在说话，都持续发送，后端 ASR 处理）
    if (this.ws && this.wsConnected && !this._closed) {
      // Float32Array → Int16Array (PCM 16-bit)
      const pcmData = this._floatTo16BitPCM(buffer);
      try {
        this.ws.send(pcmData);
      } catch (e) {
        // 忽略发送错误
      }
    }

    // VAD 检测（用于打断判断）
    if (this.vadEnabled) {
      this._processVAD(volume);
    }
  }

  _processVAD(volume) {
    const now = Date.now();

    if (volume > this.vadThreshold) {
      // 检测到声音
      if (!this.isSpeech) {
        this.isSpeech = true;
        this.speechStartTime = now;
      }
      this.silenceStartTime = now;

      // AI 正在说话时用户开口 → 自动打断
      if (this.state === "speaking" && !this._interrupted) {
        const speechDuration = now - this.speechStartTime;
        if (speechDuration > 150) { // 持续 150ms 以上才认为是真的在说话
          console.log("[VAD] 检测到用户说话，打断 AI");
          this._interrupted = true;
          this.interrupt();
          setTimeout(() => { this._interrupted = false; }, 500);
        }
      }
    } else {
      // 静音
      if (this.isSpeech) {
        const speechDuration = now - this.speechStartTime;
        const silenceDuration = now - this.silenceStartTime;

        if (silenceDuration > this.vadSilenceTimeout) {
          // 静音超时，语音结束
          this.isSpeech = false;
          if (speechDuration < this.vadMinSpeechDuration) {
            // 太短，忽略
          }
        }
      } else {
        this.silenceStartTime = now;
      }
    }
  }

  _floatTo16BitPCM(input) {
    const output = new ArrayBuffer(input.length * 2);
    const view = new DataView(output);
    for (let i = 0; i < input.length; i++) {
      const s = Math.max(-1, Math.min(1, input[i]));
      view.setInt16(i * 2, s < 0 ? s * 0x8000 : s * 0x7fff, true);
    }
    return output;
  }

  // ============================================================
  // WebSocket 连接
  // ============================================================

  async _connectWebSocket() {
    return new Promise((resolve, reject) => {
      try {
        this.ws = new WebSocket(this.wsUrl);
      } catch (err) {
        reject(err);
        return;
      }

      this.ws.binaryType = "arraybuffer";

      this.ws.onopen = () => {
        console.log("[VoiceCall] WebSocket 已连接");
        this.wsConnected = true;
        resolve();
      };

      this.ws.onmessage = (event) => {
        if (this._closed) return;

        if (typeof event.data === "string") {
          // JSON 控制消息
          try {
            const msg = JSON.parse(event.data);
            this._handleServerMessage(msg);
          } catch (e) {
            console.warn("[VoiceCall] JSON 解析失败:", e);
          }
        } else if (event.data instanceof ArrayBuffer) {
          // 二进制音频帧 → 播放
          this._handleAudioChunk(event.data);
        }
      };

      this.ws.onclose = (event) => {
        console.log(`[VoiceCall] WebSocket 关闭: code=${event.code}, reason=${event.reason}`);
        this.wsConnected = false;
        if (!this._closed) {
          this.onError(`连接断开: ${event.reason || event.code}`);
          this._cleanup();
          this._setState("disconnected");
        }
      };

      this.ws.onerror = (err) => {
        console.error("[VoiceCall] WebSocket 错误:", err);
        if (!this.wsConnected) {
          reject(new Error("WebSocket 连接失败"));
        }
      };
    });
  }

  _handleServerMessage(msg) {
    switch (msg.type) {
      case "state":
        this._setState(msg.state);
        break;

      case "asr_partial":
        this.onAsrPartial(msg.text);
        break;

      case "asr_final":
        this.onAsrFinal(msg.text);
        this.transcript.push({
          role: "user",
          content: msg.text,
          timestamp: new Date().toLocaleTimeString(),
        });
        break;

      case "ai_reply":
        this.onAiReply(msg.text);
        this.transcript.push({
          role: "ai",
          content: msg.text,
          timestamp: new Date().toLocaleTimeString(),
        });
        break;

      case "tts_interrupt":
        this._stopPlayback();
        break;

      case "call_ended":
        console.log("[VoiceCall] 通话结束，记录数:", msg.transcript?.length);
        if (msg.session_id) {
          this.sessionId = msg.session_id;
        }
        this.onCallEnded(msg.transcript || this.transcript);
        break;

      case "error":
        console.error("[VoiceCall] 服务端错误:", msg.message);
        this.onError(msg.message);
        break;

      case "pong":
        break;
    }
  }

  // ============================================================
  // 流式音频播放
  // ============================================================

  _handleAudioChunk(arrayBuffer) {
    if (this._closed) return;

    // 初始化播放上下文
    if (!this.playContext) {
      this.playContext = new (window.AudioContext || window.webkitAudioContext)({
        sampleRate: 16000,
      });
      this.playGainNode = this.playContext.createGain();
      this.playGainNode.gain.value = 1.0;
      this.playGainNode.connect(this.playContext.destination);
    }

    // 恢复被浏览器暂停的 AudioContext
    if (this.playContext.state === "suspended") {
      this.playContext.resume();
    }

    // Int16 PCM → Float32
    const pcmData = new Int16Array(arrayBuffer);
    const floatData = new Float32Array(pcmData.length);
    for (let i = 0; i < pcmData.length; i++) {
      floatData[i] = pcmData[i] / 32768;
    }

    // 创建 AudioBuffer
    const audioBuffer = this.playContext.createBuffer(
      1,
      floatData.length,
      16000
    );
    audioBuffer.getChannelData(0).set(floatData);

    // 加入播放队列
    this.audioQueue.push(audioBuffer);

    // 如果没在播放，开始播放
    if (!this.isPlaying) {
      this._playNext();
    }
  }

  _playNext() {
    if (this.audioQueue.length === 0 || this._closed) {
      this.isPlaying = false;
      return;
    }

    this.isPlaying = true;
    const buffer = this.audioQueue.shift();

    const source = this.playContext.createBufferSource();
    source.buffer = buffer;
    source.connect(this.playGainNode);

    source.onended = () => {
      if (this.currentSource === source) {
        this.currentSource = null;
      }
      // 播放下一块
      if (this.audioQueue.length > 0) {
        this._playNext();
      } else {
        this.isPlaying = false;
      }
    };

    this.currentSource = source;
    source.start(0);
  }

  _stopPlayback() {
    // 停止当前播放并清空队列
    if (this.currentSource) {
      try {
        this.currentSource.stop();
      } catch (e) {
        // 忽略
      }
      this.currentSource = null;
    }
    this.audioQueue = [];
    this.isPlaying = false;
  }

  // ============================================================
  // 状态与清理
  // ============================================================

  _setState(newState) {
    if (this.state === newState) return;
    const old = this.state;
    this.state = newState;
    console.log(`[VoiceCall] 状态: ${old} → ${newState}`);
    this.onStateChange(newState);
  }

  _cleanup() {
    this._closed = true;

    // 停止播放
    this._stopPlayback();

    // 关闭 WebSocket
    if (this.ws) {
      try {
        this.ws.close();
      } catch (e) {
        // 忽略
      }
      this.ws = null;
    }
    this.wsConnected = false;

    // 停止音频采集
    if (this.workletNode) {
      try {
        this.workletNode.disconnect();
        this.workletNode.port.onmessage = null;
      } catch (e) {
        // 忽略
      }
      this.workletNode = null;
    }

    if (this.micGainNode) {
      try {
        this.micGainNode.disconnect();
      } catch (e) {
        // 忽略
      }
      this.micGainNode = null;
    }

    if (this.sourceNode) {
      try {
        this.sourceNode.disconnect();
      } catch (e) {
        // 忽略
      }
      this.sourceNode = null;
    }

    if (this.mediaStream) {
      this.mediaStream.getTracks().forEach(track => track.stop());
      this.mediaStream = null;
    }

    if (this.audioContext) {
      try {
        this.audioContext.close();
      } catch (e) {
        // 忽略
      }
      this.audioContext = null;
    }

    if (this.playContext) {
      try {
        this.playContext.close();
      } catch (e) {
        // 忽略
      }
      this.playContext = null;
    }

    console.log("[VoiceCall] 资源已清理");
  }

  // 获取通话时长
  getDuration() {
    if (!this._callStartTime) return 0;
    return Math.floor((Date.now() - this._callStartTime) / 1000);
  }
}

// 导出（支持浏览器全局和模块）
if (typeof module !== "undefined" && module.exports) {
  module.exports = VoiceCallModule;
}
if (typeof window !== "undefined") {
  window.VoiceCallModule = VoiceCallModule;
}
