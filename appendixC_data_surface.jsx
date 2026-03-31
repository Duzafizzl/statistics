/**
 * appendixC_data_surface.jsx - Interactive 3D visualization for Appendix-C monthly and tag data.
 * Created: 2026-03-31
 * Last updated: 2026-03-31
 */
import { useMemo, useRef, useEffect, useCallback, useState } from "react";
import * as THREE from "three";

const APPENDIX_C_TAXONOMY_DATA = {
  statistical_reference: {
    monthly_baseline: {
      "2025-11": { sessions: 80, messages: 1354, avg_messages_per_session: 16.9, avg_messages_per_day: 123.1 },
      "2025-12": { sessions: 279, messages: 2297, avg_messages_per_session: 8.2, avg_messages_per_day: 74.1 },
      "2026-01": { sessions: 193, messages: 2043, avg_messages_per_session: 10.6, avg_messages_per_day: 92.9 },
    },
    tag_baselines: {
      memory_operations_pct: 60.0,
      identity_claims_pct: 40.0,
      cross_session_reference_pct: 25.0,
      preferences_pct: 30.0,
      meta_reflection_pct: 20.0,
      self_repair_pct: 15.0,
      meta_diagnostics_pct: 5.0,
    },
  },
};

function log(level, moduleName, message, context = {}) {
  const payload = {
    timestamp: new Date().toISOString(),
    level,
    module: moduleName,
    message,
    ...context,
  };
  const txt = JSON.stringify(payload);
  if (level === "ERROR" || level === "CRITICAL") {
    console.error(txt);
    return;
  }
  if (level === "WARN") {
    console.warn(txt);
    return;
  }
  console.info(txt);
}

function logError(moduleName, message, error, context = {}) {
  log("ERROR", moduleName, message, {
    error_code: context.error_code || "UNEXPECTED_ERROR",
    error_message: error?.message || String(error),
    stack_trace: error?.stack || "stack_unavailable",
    ...context,
  });
}

function infernoColor(t) {
  const clamped = Math.max(0, Math.min(1, t));
  const stops = [
    [0.0, 0.02, 0.01, 0.12],
    [0.15, 0.09, 0.03, 0.33],
    [0.3, 0.26, 0.01, 0.52],
    [0.45, 0.48, 0.03, 0.53],
    [0.6, 0.68, 0.14, 0.38],
    [0.75, 0.86, 0.34, 0.18],
    [0.9, 0.96, 0.6, 0.06],
    [1.0, 0.99, 0.91, 0.48],
  ];
  let i = 0;
  for (; i < stops.length - 2; i += 1) {
    if (clamped <= stops[i + 1][0]) break;
  }
  const s0 = stops[i];
  const s1 = stops[i + 1];
  const f = (clamped - s0[0]) / (s1[0] - s0[0]);
  return [s0[1] + f * (s1[1] - s0[1]), s0[2] + f * (s1[2] - s0[2]), s0[3] + f * (s1[3] - s0[3])];
}

function toFrames(taxonomyData, metricPreset) {
  const startTs = performance.now();
  const monthly = taxonomyData?.statistical_reference?.monthly_baseline || {};
  const tags = taxonomyData?.statistical_reference?.tag_baselines || {};

  const monthKeys = Object.keys(monthly).sort();
  const tagRows = Object.entries(tags).map(([k, v]) => ({
    name: k.replace("_pct", ""),
    baselinePct: Number(v) || 0,
  }));
  const maxSessions = Math.max(1, ...monthKeys.map((m) => Number(monthly[m]?.sessions) || 0));
  const maxMessages = Math.max(1, ...monthKeys.map((m) => Number(monthly[m]?.messages) || 0));
  const maxAvgMsg = Math.max(1, ...monthKeys.map((m) => Number(monthly[m]?.avg_messages_per_session) || 0));
  const maxAvgDay = Math.max(1, ...monthKeys.map((m) => Number(monthly[m]?.avg_messages_per_day) || 0));

  const rawRows = monthKeys.map((month) => {
    const monthData = monthly[month] || {};
    const factors = {
      sessions: (Number(monthData.sessions) || 0) / maxSessions,
      messages: (Number(monthData.messages) || 0) / maxMessages,
      avg_messages_per_session: (Number(monthData.avg_messages_per_session) || 0) / maxAvgMsg,
      avg_messages_per_day: (Number(monthData.avg_messages_per_day) || 0) / maxAvgDay,
    };
    const factor = factors[metricPreset] || factors.messages;
    return {
      month,
      values: tagRows.map((tagRow) => Math.max(0, Math.min(100, tagRow.baselinePct * (0.5 + factor * 0.9)))),
    };
  });

  const interpolatedFrames = [];
  const stepsBetween = 24;
  for (let i = 0; i < rawRows.length - 1; i += 1) {
    const a = rawRows[i];
    const b = rawRows[i + 1];
    for (let s = 0; s < stepsBetween; s += 1) {
      const t = s / stepsBetween;
      interpolatedFrames.push({
        t: i + t,
        monthLabel: `${a.month} -> ${b.month}`,
        values: a.values.map((av, idx) => av + (b.values[idx] - av) * t),
      });
    }
  }
  if (rawRows.length > 0) {
    const last = rawRows[rawRows.length - 1];
    interpolatedFrames.push({
      t: rawRows.length - 1,
      monthLabel: last.month,
      values: [...last.values],
    });
  }

  log("DEBUG", "toFrames", "Framedaten erzeugt", {
    duration_ms: Math.round(performance.now() - startTs),
    month_count: monthKeys.length,
    tag_count: tagRows.length,
    frame_count: interpolatedFrames.length,
    metric_preset: metricPreset,
  });

  return {
    months: monthKeys,
    tags: tagRows.map((t) => t.name),
    frames: interpolatedFrames,
    rawRows,
  };
}

export default function AppendixCDataSurface({ taxonomyData = APPENDIX_C_TAXONOMY_DATA }) {
  const mountRef = useRef(null);
  const sceneRef = useRef({});
  const traceIdRef = useRef(`appendixC-${Date.now()}`);
  const animRef = useRef(null);
  const [metricPreset, setMetricPreset] = useState("messages");
  const [animating, setAnimating] = useState(false);
  const [timeSlice, setTimeSlice] = useState(0);
  const [speed, setSpeed] = useState(1.2);
  const [showWire, setShowWire] = useState(true);

  const matrix = useMemo(() => toFrames(taxonomyData, metricPreset), [taxonomyData, metricPreset]);

  const buildSurface = useCallback((sliceEnd, withWire = true) => {
    const opStart = performance.now();
    const sc = sceneRef.current;
    if (!sc.scene || !sc.renderer || !sc.camera || !matrix.frames?.length) {
      log("WARN", "buildSurface", "Build skipped due to incomplete scene", {
        trace_id: traceIdRef.current,
        has_scene: !!sc.scene,
        has_renderer: !!sc.renderer,
        has_camera: !!sc.camera,
        frame_count: matrix.frames?.length || 0,
      });
      return;
    }

    try {
      const cols = matrix.tags.length;
      const framesCount = typeof sliceEnd === "number"
        ? Math.max(2, Math.min(matrix.frames.length, sliceEnd + 1))
        : matrix.frames.length;
      const maxValue = 100;
      const scaleX = 6 / Math.max(1, cols - 1);
      const scaleZ = 6 / Math.max(1, framesCount - 1);
      const scaleY = 3 / maxValue;

      if (sc.surfaceMesh) {
        sc.scene.remove(sc.surfaceMesh);
        sc.surfaceMesh.geometry.dispose();
        sc.surfaceMesh.material.dispose();
      }
      if (sc.wireMesh) {
        sc.scene.remove(sc.wireMesh);
        sc.wireMesh.geometry.dispose();
        sc.wireMesh.material.dispose();
      }

      const geometry = new THREE.BufferGeometry();
      const vertices = [];
      const colors = [];
      const indices = [];

      for (let fi = 0; fi < framesCount; fi += 1) {
        const frame = matrix.frames[fi];
        for (let ci = 0; ci < cols; ci += 1) {
          const x = -3 + ci * scaleX;
          const z = -3 + fi * scaleZ;
          const value = frame.values[ci];
          const y = value * scaleY;
          vertices.push(x, y, z);
          const col = infernoColor(value / maxValue);
          colors.push(col[0], col[1], col[2]);
        }
      }

      for (let fi = 0; fi < framesCount - 1; fi += 1) {
        for (let ci = 0; ci < cols - 1; ci += 1) {
          const a = fi * cols + ci;
          const b = a + 1;
          const d = (fi + 1) * cols + ci;
          const e = d + 1;
          indices.push(a, d, b);
          indices.push(b, d, e);
        }
      }

      geometry.setAttribute("position", new THREE.Float32BufferAttribute(vertices, 3));
      geometry.setAttribute("color", new THREE.Float32BufferAttribute(colors, 3));
      geometry.setIndex(indices);
      geometry.computeVertexNormals();

      const material = new THREE.MeshLambertMaterial({ vertexColors: true, side: THREE.DoubleSide });
      const mesh = new THREE.Mesh(geometry, material);
      sc.scene.add(mesh);
      sc.surfaceMesh = mesh;

      if (withWire) {
        const wireGeom = new THREE.BufferGeometry();
        const wireVerts = [];
        const colStep = Math.max(1, Math.floor(cols / 8));
        const frameStep = Math.max(1, Math.floor(framesCount / 18));

        for (let fi = 0; fi < framesCount; fi += frameStep) {
          for (let ci = 0; ci < cols - 1; ci += 1) {
            const x1 = -3 + ci * scaleX;
            const x2 = -3 + (ci + 1) * scaleX;
            const z = -3 + fi * scaleZ;
            const y1 = matrix.frames[fi].values[ci] * scaleY;
            const y2 = matrix.frames[fi].values[ci + 1] * scaleY;
            wireVerts.push(x1, y1 + 0.003, z, x2, y2 + 0.003, z);
          }
        }

        for (let ci = 0; ci < cols; ci += colStep) {
          for (let fi = 0; fi < framesCount - 1; fi += 1) {
            const x = -3 + ci * scaleX;
            const z1 = -3 + fi * scaleZ;
            const z2 = -3 + (fi + 1) * scaleZ;
            const y1 = matrix.frames[fi].values[ci] * scaleY;
            const y2 = matrix.frames[fi + 1].values[ci] * scaleY;
            wireVerts.push(x, y1 + 0.003, z1, x, y2 + 0.003, z2);
          }
        }

        wireGeom.setAttribute("position", new THREE.Float32BufferAttribute(wireVerts, 3));
        const wireMat = new THREE.LineBasicMaterial({ color: 0xffffff, transparent: true, opacity: 0.12 });
        const wireLines = new THREE.LineSegments(wireGeom, wireMat);
        sc.scene.add(wireLines);
        sc.wireMesh = wireLines;
      }

      sc.renderer.render(sc.scene, sc.camera);
      log("DEBUG", "buildSurface", "Surface aktualisiert", {
        trace_id: traceIdRef.current,
        frame_count: framesCount,
        tag_count: cols,
        with_wire: withWire,
        duration_ms: Math.round(performance.now() - opStart),
      });
    } catch (error) {
      logError("buildSurface", "Surface build failed", error, {
        trace_id: traceIdRef.current,
        error_code: "SURFACE_BUILD_FAILED",
        slice_end: sliceEnd,
      });
    }
  }, [matrix]);

  useEffect(() => {
    const opStart = performance.now();
    const container = mountRef.current;
    if (!container) {
      log("ERROR", "AppendixCDataSurface.useEffect:init", "Container unavailable", {
        trace_id: traceIdRef.current,
        error_code: "MOUNT_CONTAINER_MISSING",
      });
      return undefined;
    }

    log("INFO", "AppendixCDataSurface.useEffect:init", "Initializing Three.js scene", {
      trace_id: traceIdRef.current,
      month_count: matrix.months.length,
      tag_count: matrix.tags.length,
      frame_count: matrix.frames.length,
    });

    const w = container.clientWidth || 960;
    const h = container.clientHeight || 640;
    const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: false });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    renderer.setSize(w, h);
    renderer.setClearColor(0x0a0e1a);
    container.appendChild(renderer.domElement);

    const scene = new THREE.Scene();
    scene.fog = new THREE.FogExp2(0x0a0e1a, 0.03);

    const camera = new THREE.PerspectiveCamera(45, w / h, 0.1, 100);
    camera.position.set(7, 6, 7);
    camera.lookAt(0, 0, 0);

    scene.add(new THREE.AmbientLight(0x889aaa, 1.2));
    const dir = new THREE.DirectionalLight(0xffeedd, 0.8);
    dir.position.set(6, 8, 5);
    scene.add(dir);

    const axesMat = new THREE.LineBasicMaterial({ color: 0x556688, transparent: true, opacity: 0.5 });
    const makeAxis = (from, to) => {
      const g = new THREE.BufferGeometry().setFromPoints([new THREE.Vector3(...from), new THREE.Vector3(...to)]);
      return new THREE.Line(g, axesMat);
    };
    scene.add(makeAxis([-3, 0, -3], [3, 0, -3]));
    scene.add(makeAxis([-3, 0, -3], [-3, 0, 3]));
    scene.add(makeAxis([-3, 0, -3], [-3, 3.8, -3]));

    function makeLabel(text, pos, size = 0.3) {
      const canvas = document.createElement("canvas");
      canvas.width = 320;
      canvas.height = 64;
      const ctx = canvas.getContext("2d");
      if (!ctx) return;
      ctx.fillStyle = "#77cccc";
      ctx.font = "bold 32px monospace";
      ctx.textAlign = "center";
      ctx.fillText(text, 160, 44);
      const tex = new THREE.CanvasTexture(canvas);
      const mat = new THREE.SpriteMaterial({ map: tex, transparent: true });
      const sprite = new THREE.Sprite(mat);
      sprite.position.set(...pos);
      sprite.scale.set(size * 4.5, size, 1);
      scene.add(sprite);
    }
    makeLabel("Tags", [0, -0.35, -3.5]);
    makeLabel("Timeline", [-3.6, -0.35, 0]);
    makeLabel("Intensity", [-3.9, 1.8, -3]);

    sceneRef.current = { scene, camera, renderer, traceId: traceIdRef.current };
    buildSurface(timeSlice, showWire);

    let isDragging = false;
    let prevX = 0;
    let prevY = 0;
    let theta = Math.PI / 4;
    let phi = Math.PI / 5;
    let radius = 11;

    const updateCamera = () => {
      camera.position.x = radius * Math.sin(theta) * Math.cos(phi);
      camera.position.y = radius * Math.sin(phi) + 1;
      camera.position.z = radius * Math.cos(theta) * Math.cos(phi);
      camera.lookAt(0, 0.8, 0);
      renderer.render(scene, camera);
    };
    updateCamera();

    const onDown = (e) => {
      isDragging = true;
      const p = e.touches ? e.touches[0] : e;
      prevX = p.clientX;
      prevY = p.clientY;
    };
    const onMove = (e) => {
      if (!isDragging) return;
      const p = e.touches ? e.touches[0] : e;
      theta -= (p.clientX - prevX) * 0.005;
      phi = Math.max(0.05, Math.min(Math.PI / 2.2, phi + (p.clientY - prevY) * 0.005));
      prevX = p.clientX;
      prevY = p.clientY;
      updateCamera();
    };
    const onUp = () => { isDragging = false; };
    const onWheel = (e) => {
      e.preventDefault();
      radius = Math.max(5, Math.min(20, radius + e.deltaY * 0.01));
      updateCamera();
    };

    const el = renderer.domElement;
    el.addEventListener("mousedown", onDown);
    el.addEventListener("mousemove", onMove);
    el.addEventListener("mouseup", onUp);
    el.addEventListener("mouseleave", onUp);
    el.addEventListener("touchstart", onDown, { passive: true });
    el.addEventListener("touchmove", onMove, { passive: true });
    el.addEventListener("touchend", onUp);
    el.addEventListener("wheel", onWheel, { passive: false });

    const onResize = () => {
      const nw = container.clientWidth || 960;
      const nh = container.clientHeight || 640;
      camera.aspect = nw / nh;
      camera.updateProjectionMatrix();
      renderer.setSize(nw, nh);
      updateCamera();
      log("DEBUG", "AppendixCDataSurface.onResize", "Viewport angepasst", {
        trace_id: traceIdRef.current,
        width: nw,
        height: nh,
      });
    };
    window.addEventListener("resize", onResize);

    log("INFO", "AppendixCDataSurface.useEffect:init", "Scene initialized", {
      trace_id: traceIdRef.current,
      duration_ms: Math.round(performance.now() - opStart),
    });

    return () => {
      const cleanupStart = performance.now();
      window.removeEventListener("resize", onResize);
      el.removeEventListener("mousedown", onDown);
      el.removeEventListener("mousemove", onMove);
      el.removeEventListener("mouseup", onUp);
      el.removeEventListener("mouseleave", onUp);
      el.removeEventListener("touchstart", onDown);
      el.removeEventListener("touchmove", onMove);
      el.removeEventListener("touchend", onUp);
      el.removeEventListener("wheel", onWheel);
      if (sceneRef.current.surfaceMesh) {
        sceneRef.current.surfaceMesh.geometry.dispose();
        sceneRef.current.surfaceMesh.material.dispose();
      }
      if (sceneRef.current.wireMesh) {
        sceneRef.current.wireMesh.geometry.dispose();
        sceneRef.current.wireMesh.material.dispose();
      }
      renderer.dispose();
      if (renderer.domElement.parentNode === container) {
        container.removeChild(renderer.domElement);
      }
      log("INFO", "AppendixCDataSurface.useEffect:cleanup", "Resources released", {
        trace_id: traceIdRef.current,
        duration_ms: Math.round(performance.now() - cleanupStart),
      });
    };
  }, [matrix, buildSurface, showWire, timeSlice]);

  useEffect(() => {
    buildSurface(timeSlice, showWire);
  }, [timeSlice, showWire, matrix, buildSurface]);

  useEffect(() => {
    if (!animating) {
      if (animRef.current) cancelAnimationFrame(animRef.current);
      return undefined;
    }
    log("INFO", "AppendixCDataSurface.animation", "Animation gestartet", {
      trace_id: traceIdRef.current,
      speed,
      frame_count: matrix.frames.length,
    });
    let frame = timeSlice;
    const total = Math.max(1, matrix.frames.length);
    let accum = 0;
    const step = () => {
      accum += speed;
      if (accum >= 1) {
        frame = (frame + Math.floor(accum)) % total;
        accum %= 1;
        setTimeSlice(frame);
      }
      animRef.current = requestAnimationFrame(step);
    };
    animRef.current = requestAnimationFrame(step);
    return () => {
      if (animRef.current) cancelAnimationFrame(animRef.current);
      log("INFO", "AppendixCDataSurface.animation", "Animation gestoppt", {
        trace_id: traceIdRef.current,
      });
    };
  }, [animating, speed, matrix.frames.length, timeSlice]);

  const sliderStyle = {
    appearance: "none",
    WebkitAppearance: "none",
    width: "100%",
    height: 4,
    borderRadius: 2,
    background: "linear-gradient(90deg, #2a1040, #cc6622, #ffcc44)",
    outline: "none",
    cursor: "pointer",
  };

  return (
    <div
      style={{
        width: "100%",
        height: "100vh",
        background: "#0a0e1a",
        display: "flex",
        flexDirection: "column",
        color: "#88bbcc",
        fontFamily: "'JetBrains Mono', monospace",
      }}
    >
      <div style={{ padding: "14px 18px 8px", borderBottom: "1px solid #1a2040" }}>
        <h2 style={{ margin: 0, fontSize: 16 }}>Appendix C Data Surface</h2>
        <div style={{ fontSize: 11, opacity: 0.65 }}>
          X = tag baselines, Z = interpolated timeline, Y = intensity by metric preset
        </div>
      </div>
      <div ref={mountRef} style={{ flex: 1, minHeight: 0 }} />

      <div style={{
        padding: "12px 20px 16px",
        borderTop: "1px solid #1a2040",
        background: "linear-gradient(180deg, #0c1020, #0a0e1a)",
      }}>
        <div style={{ display: "flex", gap: 12, flexWrap: "wrap", alignItems: "center", marginBottom: 10 }}>
          <div style={{ flex: "1 1 220px" }}>
            <label style={{ fontSize: 10, textTransform: "uppercase", letterSpacing: 1, opacity: 0.6 }}>
              Animation Speed = {speed.toFixed(1)}x
            </label>
            <input
              type="range"
              min="0.2"
              max="5"
              step="0.1"
              value={speed}
              onChange={(e) => {
                const v = parseFloat(e.target.value);
                setSpeed(v);
                log("INFO", "AppendixCDataSurface.ui.speed", "Speed changed", {
                  trace_id: traceIdRef.current,
                  speed: v,
                });
              }}
              style={sliderStyle}
            />
          </div>
          <div style={{ flex: "0 0 auto", display: "flex", gap: 6 }}>
            {["sessions", "messages", "avg_messages_per_session", "avg_messages_per_day"].map((p) => (
              <button
                key={p}
                onClick={() => {
                  setMetricPreset(p);
                  setTimeSlice(0);
                  log("INFO", "AppendixCDataSurface.ui.preset", "Metric preset changed", {
                    trace_id: traceIdRef.current,
                    metric_preset: p,
                  });
                }}
                style={{
                  padding: "5px 10px",
                  fontSize: 10,
                  fontFamily: "inherit",
                  border: metricPreset === p ? "1px solid #cc8844" : "1px solid #223",
                  borderRadius: 4,
                  background: metricPreset === p ? "#1a1428" : "transparent",
                  color: metricPreset === p ? "#ffcc66" : "#556",
                  cursor: "pointer",
                }}
              >
                {p.replaceAll("_", " ")}
              </button>
            ))}
          </div>
        </div>

        <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
          <button
            onClick={() => setAnimating((a) => !a)}
            style={{
              padding: "6px 14px",
              fontSize: 11,
              fontFamily: "inherit",
              border: "1px solid #cc8844",
              borderRadius: 4,
              background: animating ? "#331a08" : "transparent",
              color: "#ffcc66",
              cursor: "pointer",
            }}
          >
            {animating ? "■ Stop" : "▶ Animate"}
          </button>

          <button
            onClick={() => {
              setShowWire((w) => !w);
              log("INFO", "AppendixCDataSurface.ui.wire", "Wireframe toggled", {
                trace_id: traceIdRef.current,
                enabled: !showWire,
              });
            }}
            style={{
              padding: "6px 12px",
              fontSize: 11,
              fontFamily: "inherit",
              border: "1px solid #335577",
              borderRadius: 4,
              background: showWire ? "#102030" : "transparent",
              color: "#88bbcc",
              cursor: "pointer",
            }}
          >
            {showWire ? "Wire On" : "Wire Off"}
          </button>

          <div style={{ flex: 1 }}>
            <input
              type="range"
              min="0"
              max={Math.max(0, matrix.frames.length - 1)}
              step="1"
              value={Math.min(timeSlice, Math.max(0, matrix.frames.length - 1))}
              onChange={(e) => setTimeSlice(parseInt(e.target.value, 10))}
              style={sliderStyle}
            />
          </div>
          <div style={{ minWidth: 120, fontSize: 10, opacity: 0.55, textAlign: "right" }}>
            Frame {Math.min(timeSlice + 1, matrix.frames.length)}/{matrix.frames.length}
          </div>
        </div>
      </div>

      <style>{`
        input[type=range]::-webkit-slider-thumb {
          -webkit-appearance: none;
          width: 14px; height: 14px;
          border-radius: 50%;
          background: #ffcc44;
          border: 2px solid #0a0e1a;
          cursor: pointer;
        }
        input[type=range]::-moz-range-thumb {
          width: 14px; height: 14px;
          border-radius: 50%;
          background: #ffcc44;
          border: 2px solid #0a0e1a;
          cursor: pointer;
        }
      `}</style>
    </div>
  );
}
