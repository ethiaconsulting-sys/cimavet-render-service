import express from "express";
import { createSupabaseAdmin, runCimavetJob } from "./cimavet.js";

const app = express();
app.use(express.json({ limit: "1mb" }));

const port = Number(process.env.PORT || 3000);
const jobSecret = process.env.JOB_SECRET || "";
const runningJobs = new Set();

function requireSecret(req, res, next) {
  if (!jobSecret) return next();
  if (req.header("x-job-secret") !== jobSecret) {
    return res.status(401).json({ ok: false, error: "Unauthorized" });
  }
  return next();
}

async function createJobRecord(supabase) {
  const { data, error } = await supabase
    .from("ctrl_cimavet_trabajos")
    .insert({
      estado_descarga_fichero_xml: "iniciado",
      estado_carga_diccionarios: "nuevo",
      estado_carga_prescripciones: "nuevo",
      estado_carga_db: "iniciado",
      chunks_procesados: 0,
      chunks_error: 0
    })
    .select("id_trabajo")
    .single();

  if (error) {
    throw new Error(`[ctrl_cimavet_trabajos] ${error.message}`);
  }

  return data.id_trabajo;
}

app.get("/health", async (_req, res) => {
  res.json({
    ok: true,
    service: "cimavet-render-service"
  });
});

app.post("/jobs/run", requireSecret, async (req, res) => {
  if (runningJobs.size > 0) {
    return res.status(409).json({
      ok: false,
      error: "Another CIMAVET job is already running"
    });
  }

  try {
    const supabase = createSupabaseAdmin();
    const jobId = await createJobRecord(supabase);
    runningJobs.add(jobId);

    runCimavetJob(jobId, req.body)
      .then(async (summary) => {
        const client = createSupabaseAdmin();
        await client
          .from("ctrl_cimavet_trabajos")
          .update({ estado_carga_db: "finalizado" })
          .eq("id_trabajo", jobId);
        console.log(JSON.stringify({ level: "info", msg: "job completed", summary }));
      })
      .catch(async (error) => {
        const client = createSupabaseAdmin();
        await client
          .from("ctrl_cimavet_trabajos")
          .update({ estado_carga_db: "abortado", chunks_error: 1 })
          .eq("id_trabajo", jobId);
        console.error(JSON.stringify({
          level: "error",
          jobId,
          message: error?.message ?? null,
          details: error?.details ?? null,
          hint: error?.hint ?? null,
          code: error?.code ?? null,
          stack: error?.stack ?? null,
          raw: error
        }, null, 2));
      })
      .finally(() => {
        runningJobs.delete(jobId);
      });

    return res.status(202).json({
      ok: true,
      job_id: jobId,
      status_url: `/jobs/${jobId}`
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error instanceof Error ? error.message : String(error)
    });
  }
});

app.get("/jobs/:jobId", requireSecret, async (req, res) => {
  try {
    const supabase = createSupabaseAdmin();
    const { data, error } = await supabase
      .from("ctrl_cimavet_trabajos")
      .select("*")
      .eq("id_trabajo", req.params.jobId)
      .single();

    if (error) {
      return res.status(404).json({ ok: false, error: error.message });
    }

    return res.json({ ok: true, job: data });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error instanceof Error ? error.message : String(error)
    });
  }
});

app.listen(port, () => {
  console.log(`cimavet-render-service listening on port ${port}`);
});
