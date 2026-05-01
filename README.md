# cimavet-render-service

Servei Node.js pensat per desplegar-se a Render i processar la càrrega CIMAVET fora de Supabase Edge.

## Què fa

- `POST /jobs/run`
  - crea un registre a `ctrl_cimavet_trabajos`
  - descarrega `prescripcionVET.zip`
  - carrega diccionaris, dades bàsiques i autoritzacions especials
  - processa `PrescripcionVET.xml` en streaming
  - persisteix les prescripcions via `public.cimavet_apply_prescription_chunk(jsonb)`
- `GET /jobs/:jobId`
  - retorna l'estat del treball des de Supabase
- `GET /health`
  - healthcheck

## Variables d'entorn

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `JOB_SECRET`
- `CIMAVET_ZIP_URL` opcional
- `CHUNK_SIZE` opcional

## Desplegament a Render

1. Puja aquesta carpeta al repo `cimavet-render-service`.
2. Crea un `Web Service` a Render apuntant al repo.
3. Defineix:
   - Build Command: `npm install`
   - Start Command: `npm start`
4. Configura les variables d'entorn.

## Exemple de crida

```bash
curl -X POST https://<el-teu-servei>.onrender.com/jobs/run \
  -H "Content-Type: application/json" \
  -H "x-job-secret: <JOB_SECRET>" \
  -d "{\"chunkSize\":25}"
```

Resposta:

```json
{
  "ok": true,
  "job_id": 123,
  "status_url": "/jobs/123"
}
```
