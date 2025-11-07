// ==============================
// CONFIGURACIÓN BASE DEL BACKEND
// ==============================
const BASE = "http://127.0.0.1:8080"; // URL del backend FastAPI

// Selección rápida de elementos
const $ = (q) => document.querySelector(q);
const output = $("#output");

// Función auxiliar para mostrar mensajes bonitos
function log(msg, type = "info") {
  const colors = { info: "#0ff", success: "#0f0", error: "#f55" };
  output.innerHTML += `<div style="color:${colors[type]}">${msg}</div>`;
  output.scrollTop = output.scrollHeight;
}

// ==============================
// 1️⃣ SUBIR ARCHIVO .DBF
// ==============================
$("#btnUpload").onclick = async () => {
  const file = $("#dbfFile").files[0];
  if (!file) {
    log("⚠️ Selecciona un archivo .DBF antes de subir.", "error");
    return;
  }

  log(`📤 Subiendo archivo: ${file.name}...`);

  const fd = new FormData();
  fd.append("file", file);

  try {
    const res = await fetch(`${BASE}/api/upload_dbf`, {
      method: "POST",
      body: fd,
    });

    const data = await res.json();
    if (res.ok) {
      log(`✅ Archivo cargado correctamente: ${data.file}`, "success");
    } else {
      log(`❌ Error al subir archivo: ${JSON.stringify(data)}`, "error");
    }
  } catch (err) {
    log(`❌ Error de conexión: ${err}`, "error");
  }
};

// ==============================
// 2️⃣ DESCUBRIR ESQUEMA (PK/FK)
// ==============================
$("#btnDiscover").onclick = async () => {
  log("🔍 Ejecutando descubrimiento de esquema...");

  try {
    const res = await fetch(`${BASE}/api/discover_schema`);
    if (!res.ok) throw new Error(`Código HTTP ${res.status}`);

    const ct = res.headers.get("content-type") || "";
    if (ct.includes("application/json")) {
      const data = await res.json();
      log(`✅ Descubrimiento completado: ${Object.keys(data).length} tablas`, "success");
    } else {
      log("✅ Descubrimiento ejecutado correctamente (schema.json generado).", "success");
    }
  } catch (err) {
    log(`❌ Error al descubrir esquema: ${err}`, "error");
  }
};

// ==============================
// 3️⃣ DESCARGAR DDL SQL
// ==============================
$("#btnDownload").onclick = () => {
  log("⬇️ Descargando archivo schema.sql...");
  window.open(`${BASE}/api/download_schema_sql`);
};

// ==============================
// 4️⃣ EJECUTAR ETL COMPLETO
// ==============================
$("#btnETL").onclick = async () => {
  log("⚙️ Ejecutando proceso ETL (esto puede tardar)...");

  try {
    const res = await fetch(`${BASE}/api/run_etl`);
    const data = await res.json();

    if (res.ok) {
      log(`✅ ETL completado: ${data.msg}`, "success");
    } else {
      log(`❌ Error durante el ETL: ${JSON.stringify(data)}`, "error");
    }
  } catch (err) {
    log(`❌ Fallo al ejecutar ETL: ${err}`, "error");
  }
};

// ==============================
// LIMPIAR SALIDA (opcional)
// ==============================
output.addEventListener("dblclick", () => {
  output.innerHTML = "";
});
