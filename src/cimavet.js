import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import unzipper from "unzipper";
import { createClient } from "@supabase/supabase-js";
import {
  CIMAVET_DICT_CONFIG,
  DEFAULT_CIMAVET_ZIP_URL,
  DEFAULT_FLUSH_TRIGGER,
  UPSERT_BATCH_SIZE
} from "./config.js";

export function createSupabaseAdmin() {
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;

  if (!url || !key) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  }

  return createClient(url, key, {
    auth: { persistSession: false }
  });
}

export function getVal(xml, tag, def = null) {
  const openTag = `<${tag}`;
  let pos = 0;
  while (pos < xml.length) {
    const start = xml.indexOf(openTag, pos);
    if (start === -1) return def;
    const nextChar = xml[start + openTag.length];
    if (![" ", ">", "\n", "\r", "\t"].includes(nextChar)) {
      pos = start + 1;
      continue;
    }
    const gt = xml.indexOf(">", start + openTag.length);
    if (gt === -1) return def;
    const closeTag = `</${tag}>`;
    const end = xml.indexOf(closeTag, gt + 1);
    if (end === -1) return def;
    const value = xml.substring(gt + 1, end).trim();
    return value || def;
  }
  return def;
}

export function getInt(xml, tag) {
  const value = getVal(xml, tag);
  if (!value) return null;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

export function getNumericString(xml, tag) {
  const value = getVal(xml, tag);
  if (!value) return null;
  const normalized = value.replace(",", ".").trim();
  if (!/^[+-]?\\d+(?:\\.\\d+)?$/.test(normalized)) return null;
  return normalized;
}

export function extractElements(xml, tag) {
  const results = [];
  const openTag = `<${tag}`;
  const closeTag = `</${tag}>`;
  let pos = 0;
  while (pos < xml.length) {
    const start = xml.indexOf(openTag, pos);
    if (start === -1) break;
    const nextChar = xml[start + openTag.length];
    if (![" ", ">", "\n", "\r", "\t"].includes(nextChar)) {
      pos = start + 1;
      continue;
    }
    const end = xml.indexOf(closeTag, start);
    if (end === -1) break;
    results.push(xml.substring(start, end + closeTag.length));
    pos = end + closeTag.length;
  }
  return results;
}

function deduplicateByKey(rows, key) {
  const seen = new Set();
  return rows.filter((row) => {
    const value = String(row[key] ?? "");
    if (!value || seen.has(value)) return false;
    seen.add(value);
    return true;
  });
}

async function upsertRows(supabase, table, rows, onConflict) {
  let total = 0;
  for (let i = 0; i < rows.length; i += UPSERT_BATCH_SIZE) {
    const chunk = rows.slice(i, i + UPSERT_BATCH_SIZE);
    if (!chunk.length) continue;
    const { error } = await supabase.from(table).upsert(chunk, { onConflict });
    if (error) throw new Error(`[${table}] ${error.message}`);
    total += chunk.length;
  }
  return total;
}

async function insertRows(supabase, table, rows) {
  let total = 0;
  for (let i = 0; i < rows.length; i += UPSERT_BATCH_SIZE) {
    const chunk = rows.slice(i, i + UPSERT_BATCH_SIZE);
    if (!chunk.length) continue;
    const { error } = await supabase.from(table).insert(chunk);
    if (error) throw new Error(`[${table}] ${error.message}`);
    total += chunk.length;
  }
  return total;
}

async function applyPrescriptionChunk(supabase, payload) {
  const { data, error } = await supabase.rpc("cimavet_apply_prescription_chunk", {
    p_payload: payload
  });
  if (error) throw error;
  if (data?.ok === false) {
    throw new Error(String(data.error || "Unknown RPC error"));
  }
  return {
    processed: Number(data?.processed ?? payload.main_rows.length),
    codNacions: Array.isArray(data?.cod_nacions) ? data.cod_nacions : []
  };
}

function buildEmptyPayload() {
  return {
    main_rows: [],
    atc_rows: [],
    via_rows: [],
    principi_rows: [],
    especie_rows: [],
    caducidad_rows: [],
    indicacio_rows: [],
    contra_rows: [],
    interaccio_rows: [],
    reaccio_rows: [],
    espera_rows: [],
    dosi_rows: []
  };
}

function clearPayload(payload) {
  for (const key of Object.keys(payload)) {
    payload[key].length = 0;
  }
}

async function downloadZipToTemp(zipUrl, jobId) {
  const response = await fetch(zipUrl);
  if (!response.ok || !response.body) {
    throw new Error(`HTTP ${response.status} downloading ZIP`);
  }

  const tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), `cimavet-${jobId}-`));
  const zipPath = path.join(tempDir, "prescripcionVET.zip");
  const file = fs.createWriteStream(zipPath);
  await pipeline(Readable.fromWeb(response.body), file);

  return { tempDir, zipPath };
}

async function openEntries(zipPath) {
  const directory = await unzipper.Open.file(zipPath);
  const map = new Map();
  for (const entry of directory.files) {
    map.set(path.basename(entry.path), entry);
  }
  return map;
}

async function readEntryAsString(entry) {
  const buffer = await entry.buffer();
  return buffer.toString("utf8");
}

async function updateJob(supabase, jobId, patch) {
  const { error } = await supabase
    .from("ctrl_cimavet_trabajos")
    .update(patch)
    .eq("id_trabajo", jobId);

  if (error) {
    throw new Error(`[ctrl_cimavet_trabajos] ${error.message}`);
  }
}

async function processDictionaries(supabase, entries, summary) {
  for (const [filename, config] of Object.entries(CIMAVET_DICT_CONFIG)) {
    const entry = entries.get(filename);
    if (!entry) continue;

    const xml = await readEntryAsString(entry);
    const rows = extractElements(xml, config.listTag).map((element) => {
      const row = {};
      for (const [xmlField, fieldCfg] of Object.entries(config.map)) {
        row[fieldCfg.col] = fieldCfg.type === "int" ? getInt(element, xmlField) : getVal(element, xmlField);
      }
      return row;
    });

    const uniqueRows = deduplicateByKey(rows, config.pk);
    summary.dictionaries[filename] = await upsertRows(supabase, config.table, uniqueRows, config.pk);
  }

  const basicsEntry = entries.get("PrescripcionVETDatosBasicos.xml");
  if (basicsEntry) {
    const xml = await readEntryAsString(basicsEntry);
    const rows = deduplicateByKey(
      extractElements(xml, "prescription").map((element) => ({
        cod_nacion: getInt(element, "cod_nacion"),
        nro_definitivo: getVal(element, "nro_definitivo"),
        nombre_med: getVal(element, "nombre_med"),
        formato: getVal(element, "formato")
      })),
      "cod_nacion"
    );
    summary.datos_basicos = await upsertRows(supabase, "cimavet_datos_basicos", rows, "cod_nacion");
  }

  const authEntry = entries.get("AUTORIZACION_ESPECIAL_MEDICAMENTOS.xml");
  if (authEntry) {
    const xml = await readEntryAsString(authEntry);
    const rows = extractElements(xml, "autorizaciones_especiales").map((element) => ({
      titular: getVal(element, "titular"),
      nombre: getVal(element, "nombre"),
      sustancia_activa: getVal(element, "sustancia_activa"),
      potencia: getVal(element, "potencia"),
      adyuvante: getVal(element, "adyuvante"),
      especie: getVal(element, "especie"),
      via_administracion: getVal(element, "via_administracion"),
      posologia: getVal(element, "posologia"),
      autorizado_desde: getVal(element, "autorizado_desde"),
      autorizado_hasta: getVal(element, "autorizado_hasta")
    }));

    const { error } = await supabase
      .from("cimavet_autorizaciones_especiales")
      .delete()
      .not("id", "is", null);

    if (error) {
      throw new Error(`[cimavet_autorizaciones_especiales] ${error.message}`);
    }

    summary.autorizaciones_especiales = await insertRows(supabase, "cimavet_autorizaciones_especiales", rows);
  }
}

async function processPrescriptionsStream(supabase, entry, options, progress) {
  const payload = buildEmptyPayload();
  const flushTrigger = options.flushTrigger || DEFAULT_FLUSH_TRIGGER;

  let xmlBuffer = "";
  let processed = 0;
  let seen = 0;
  let chunksProcessed = 0;
  let lastCodNacions = [];

  const flushIfNeeded = async (force = false) => {
    if (!force && payload.main_rows.length < flushTrigger) return;
    if (!payload.main_rows.length) return;

    const result = await applyPrescriptionChunk(supabase, payload);
    processed += result.processed;
    chunksProcessed += 1;
    lastCodNacions = result.codNacions;
    console.log(JSON.stringify({
  tag: "cimavet_flush_counts",
  chunk_number: chunksProcessed + 1,
  main_rows: payload.main_rows.length,
  atc_rows: payload.atc_rows.length,
  via_rows: payload.via_rows.length,
  principi_rows: payload.principi_rows.length,
  especie_rows: payload.especie_rows.length,
  caducidad_rows: payload.caducidad_rows.length,
  indicacio_rows: payload.indicacio_rows.length,
  contra_rows: payload.contra_rows.length,
  interaccio_rows: payload.interaccio_rows.length,
  reaccio_rows: payload.reaccio_rows.length,
  espera_rows: payload.espera_rows.length,
  dosi_rows: payload.dosi_rows.length
}));

    clearPayload(payload);

    if (progress) {
      await progress({
        processed,
        seen,
        chunksProcessed,
        lastCodNacions
      });
    }
  };

  const handlePrescription = async (xml) => {
    seen += 1;
    const codNacion = getInt(xml, "cod_nacion");
    if (!codNacion) return;

    payload.main_rows.push({
      cod_nacion: codNacion,
      nro_definitivo: getVal(xml, "nro_definitivo"),
      nombre_med: getVal(xml, "nombre_med"),
      fec_primera_aut: getVal(xml, "fec_primera_aut"),
      fecha_alta_nomenclator: getVal(xml, "fecha_alta_nomenclator"),
      cod_estado_registro_medicamento: getInt(xml, "cod_estado_registro_medicamento"),
      cod_situacion_administrativa_medicamento: getInt(xml, "cod_situacion_administrativa_medicamento"),
      cod_estado_registro_formato: getInt(xml, "cod_estado_registro_formato"),
      comercializado: getVal(xml, "comercializado"),
      posologia: getVal(xml, "posologia"),
      formato: getVal(xml, "formato"),
      contenido_total_envase: getNumericString(xml, "contenido_total_envase"),
      unidad_contenido_total_envase: getInt(xml, "unidad_contenido_total_envase"),
      prescripcion: getVal(xml, "prescripcion"),
      administracion_exclusiva_veterinario: getVal(xml, "administracion_exclusiva_veterinario"),
      administracion_bajo_control_veterinario: getVal(xml, "administracion_bajo_control_veterinario"),
      homeopatico: getVal(xml, "homeopatico"),
      contiene_edo: getVal(xml, "contiene_edo"),
      estupefaciente: getVal(xml, "estupefaciente"),
      psicotropo: getVal(xml, "psicotropo"),
      base_a_plantas: getVal(xml, "base_a_plantas"),
      ficha_tecnica: getVal(xml, "ficha_tecnica"),
      prospecto: getVal(xml, "prospecto"),
      titular: getInt(xml, "titular"),
      sw_antibiotico: getVal(xml, "sw_antibiotico"),
      sw_premezcla_medicamentosa: getVal(xml, "sw_premezcla_medicamentosa"),
      sw_dispensacion_fraccionada: getVal(xml, "sw_dispensacion_fraccionada"),
      ff_cod_forma_farmaceutica: null,
      ff_cantidad_concentracion: null,
      ff_unidad_cantidad_concentracion: null
    });

    const formaEls = extractElements(xml, "formafarmaceutica");
    if (formaEls.length > 0) {
      const forma = formaEls[0];
      const mainRow = payload.main_rows[payload.main_rows.length - 1];
      mainRow.ff_cod_forma_farmaceutica = getInt(forma, "cod_forma_farmaceutica");
      mainRow.ff_cantidad_concentracion = getVal(forma, "cantidad_concentracion");
      mainRow.ff_unidad_cantidad_concentracion = getInt(forma, "unidad_cantidad_concentracion");

      for (const via of extractElements(forma, "viasadministracion")) {
        const codVia = getInt(via, "cod_via_admin");
        if (codVia) payload.via_rows.push({ cod_nacion: codNacion, cod_via_admin: codVia });
      }

      for (const principio of extractElements(forma, "principiosactivos")) {
        const codPrincipio = getInt(principio, "cod_principio_activo");
        if (codPrincipio) {
          payload.principi_rows.push({
            cod_nacion: codNacion,
            cod_principio_activo: codPrincipio,
            cantidad: getVal(principio, "cantidad"),
            cod_unidad_cantidad: getInt(principio, "cod_unidad_cantidad")
          });
        }
      }
    }

    for (const atc of extractElements(xml, "atcvet")) {
      const code = getVal(atc, "cod_atcvet");
      if (code) payload.atc_rows.push({ cod_nacion: codNacion, cod_atcvet: code });
    }

    for (const especie of extractElements(xml, "especiesdestino")) {
      const codEsp = getInt(especie, "cod_espdes");
      if (codEsp) payload.especie_rows.push({ cod_nacion: codNacion, cod_espdes: codEsp });
    }

    for (const cad of extractElements(xml, "caducidad")) {
      payload.caducidad_rows.push({
        cod_nacion: codNacion,
        caducidad_formato: getVal(cad, "caducidad_formato"),
        caducidad_tras_primera_apertura: getVal(cad, "caducidad_tras_primera_apertura"),
        caducidad_tras_reconstitucion: getVal(cad, "caducidad_tras_reconstitucion")
      });
    }

    for (const ind of extractElements(xml, "indicacionesespecie")) {
      const idIndicacion = getInt(ind, "id_indicacion");
      if (idIndicacion) {
        payload.indicacio_rows.push({
          cod_nacion: codNacion,
          especie: getVal(ind, "especie"),
          id_indicacion: idIndicacion
        });
      }
    }

    for (const contra of extractElements(xml, "contraindicacionesespecie")) {
      const idContra = getInt(contra, "id_contraindicacion");
      if (idContra) {
        payload.contra_rows.push({
          cod_nacion: codNacion,
          especie: getVal(contra, "especie"),
          id_contraindicacion: idContra
        });
      }
    }

    for (const interaccio of extractElements(xml, "interaccionesespecie")) {
      const idInteraccio = getInt(interaccio, "id_interaccion");
      if (idInteraccio) {
        payload.interaccio_rows.push({
          cod_nacion: codNacion,
          especie: getVal(interaccio, "especie"),
          id_interaccion: idInteraccio
        });
      }
    }

    for (const reaccio of extractElements(xml, "reaccionesadversasespecie")) {
      const idSigno = getInt(reaccio, "id_signo");
      if (idSigno) {
        payload.reaccio_rows.push({
          cod_nacion: codNacion,
          especie: getVal(reaccio, "especie"),
          frecuencia: getVal(reaccio, "frecuencia"),
          id_signo: idSigno
        });
      }
    }

    for (const espera of extractElements(xml, "tiemposesperaespecie")) {
      payload.espera_rows.push({
        cod_nacion: codNacion,
        especie: getVal(espera, "especie"),
        tipo_tejido: getVal(espera, "tipo_tejido"),
        unidad: getNumericString(espera, "unidad"),
        unidad_tiempo: getVal(espera, "unidad_tiempo"),
        forma_via_admin: getVal(espera, "forma_via_admin")
      });
    }

    for (const dosi of extractElements(xml, "dosisrecomendadaespecie")) {
      payload.dosi_rows.push({
        cod_nacion: codNacion,
        especie: getVal(dosi, "especie"),
        categoria: getVal(dosi, "categoria"),
        cod_via_admin: getInt(dosi, "cod_via_admin"),
        indicacion: getVal(dosi, "indicacion"),
        dosisrecomendada: getVal(dosi, "dosisrecomendada")
      });
    }

    await flushIfNeeded();
  };

  const OPEN = "<prescription";
  const CLOSE = "</prescription>";

  const extractNext = () => {
    let pos = 0;
    while (pos < xmlBuffer.length) {
      const idx = xmlBuffer.indexOf(OPEN, pos);
      if (idx === -1) {
        const keep = OPEN.length - 1;
        xmlBuffer = xmlBuffer.length > keep ? xmlBuffer.slice(xmlBuffer.length - keep) : xmlBuffer;
        return null;
      }
      const nextChar = xmlBuffer[idx + OPEN.length];
      if (![" ", ">", "\n", "\r", "\t"].includes(nextChar)) {
        pos = idx + 1;
        continue;
      }
      const closeIdx = xmlBuffer.indexOf(CLOSE, idx);
      if (closeIdx === -1) {
        xmlBuffer = xmlBuffer.slice(idx);
        return null;
      }
      const element = xmlBuffer.substring(idx, closeIdx + CLOSE.length);
      xmlBuffer = xmlBuffer.slice(closeIdx + CLOSE.length);
      return element;
    }
    return null;
  };

  const stream = entry.stream();
  stream.setEncoding("utf8");

  for await (const chunk of stream) {
    xmlBuffer += chunk;
    let element;
    while ((element = extractNext()) !== null) {
      await handlePrescription(element);
    }
  }

  let element;
  while ((element = extractNext()) !== null) {
    await handlePrescription(element);
  }

  console.log(JSON.stringify({
  tag: "cimavet_final_payload_counts",
  main_rows: payload.main_rows.length,
  atc_rows: payload.atc_rows.length,
  via_rows: payload.via_rows.length,
  principi_rows: payload.principi_rows.length,
  especie_rows: payload.especie_rows.length,
  caducidad_rows: payload.caducidad_rows.length,
  indicacio_rows: payload.indicacio_rows.length,
  contra_rows: payload.contra_rows.length,
  interaccio_rows: payload.interaccio_rows.length,
  reaccio_rows: payload.reaccio_rows.length,
  espera_rows: payload.espera_rows.length,
  dosi_rows: payload.dosi_rows.length,
  seen,
  processed_so_far: processed,
  chunks_processed: chunksProcessed
}));

  await flushIfNeeded(true);

  return {
    seen,
    processed,
    chunksProcessed,
    lastCodNacions
  };
}

export async function runCimavetJob(jobId, input = {}) {
  const supabase = createSupabaseAdmin();
  const zipUrl = input.zipUrl || process.env.CIMAVET_ZIP_URL || DEFAULT_CIMAVET_ZIP_URL;
  const flushTrigger = Number(input.chunkSize || process.env.CHUNK_SIZE || DEFAULT_FLUSH_TRIGGER);
  const loadDictionaries = input.loadDictionaries !== false;
  const processPrescriptions = input.processPrescriptions !== false;

  const summary = {
    job_id: jobId,
    zip_url: zipUrl,
    dictionaries: {},
    datos_basicos: 0,
    autorizaciones_especiales: 0,
    prescriptions_seen: 0,
    prescriptions_processed: 0,
    chunk_flushes: 0,
    last_cod_nacions: []
  };

  const { tempDir, zipPath } = await downloadZipToTemp(zipUrl, jobId);

  try {
    await updateJob(supabase, jobId, {
      estado_descarga_fichero_xml: "finalizado",
      nombre_fichero_zip: path.basename(zipPath),
      fichero_nuevo: true,
      estado_carga_db: "procesando",
      estado_carga_diccionarios: loadDictionaries ? "procesando" : "finalizado",
      estado_carga_prescripciones: processPrescriptions ? "procesando" : "finalizado"
    });

    const entries = await openEntries(zipPath);

    if (loadDictionaries) {
      await processDictionaries(supabase, entries, summary);
      await updateJob(supabase, jobId, {
        estado_carga_diccionarios: "finalizado"
      });
    }

    if (processPrescriptions) {
      const prescriptionEntry = entries.get("PrescripcionVET.xml");
      if (!prescriptionEntry) {
        throw new Error("PrescripcionVET.xml not found inside ZIP");
      }

      const prescriptionSummary = await processPrescriptionsStream(
        supabase,
        prescriptionEntry,
        { flushTrigger },
        async ({ chunksProcessed, processed, lastCodNacions }) => {
          await updateJob(supabase, jobId, {
            total_chunks: null,
            chunks_procesados: chunksProcessed,
            estado_carga_prescripciones: "procesando"
          });
          summary.prescriptions_processed = processed;
          summary.chunk_flushes = chunksProcessed;
          summary.last_cod_nacions = lastCodNacions;
        }
      );

      summary.prescriptions_seen = prescriptionSummary.seen;
      summary.prescriptions_processed = prescriptionSummary.processed;
      summary.chunk_flushes = prescriptionSummary.chunksProcessed;
      summary.last_cod_nacions = prescriptionSummary.lastCodNacions;

      await updateJob(supabase, jobId, {
        chunks_procesados: prescriptionSummary.chunksProcessed,
        estado_carga_prescripciones: "finalizado"
      });
    }

    await updateJob(supabase, jobId, {
      estado_carga_db: "finalizado"
    });

    return summary;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    try {
      await updateJob(supabase, jobId, {
        estado_carga_diccionarios: "abortado",
        estado_carga_prescripciones: "abortado",
        estado_carga_db: "abortado"
      });
    } catch {
      // noop
    }
    throw new Error(message);
  } finally {
    await fs.promises.rm(tempDir, { recursive: true, force: true });
  }
}
