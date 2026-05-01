# cimavet-render-service

## Rol
API Node.js desplegada a Render que processa la carrega CIMAVET (XML AEMPS)
fora de Supabase Edge Functions (massa lent per al volum de dades).

## Stack
- Node.js igual o superior a 20, ESM, Express, @supabase/supabase-js, pg, unzipper
- Deploy: Render starter plan | autoDeploy: true des de GitHub main
- GitHub: ethiaconsulting-sys/cimavet-render-service (Public)

## Endpoints
- POST /jobs/run     descarrega ZIP AEMPS, processa XML, persists via RPC
- GET  /jobs/:jobId  estat del job
- GET  /health       healthcheck

## Variables d'entorn (cal configurar a Render)
- SUPABASE_URL
- SUPABASE_SERVICE_ROLE_KEY
- JOB_SECRET
- CIMAVET_ZIP_URL (opcional)
- CHUNK_SIZE (opcional)

## Funcio RPC clau
public.cimavet_apply_prescription_chunk(jsonb) -- persisteix prescripcions en chunks

## Estat
- Ultim push GitHub: fa 29 dies (SINCRONITZAR!)
- Render: autoDeploy actiu, pla starter

## Com reprendre
1. git pull per sincronitzar amb GitHub
2. Verificar que render.yaml reflecteix la configuracio actual
3. Veure tasques pendents a docs/ADR.md

## Context en cascada
Claude Code llegeix automaticament en aquest ordre:
1. C:\dev\.claude\CLAUDE.md                          (globals: agents, skills)
2. C:\dev\clients\ideant\_workspace\CLAUDE.md        (client: Ideant, stack, infraestructura)
3. Aquest CLAUDE.md                                  (projecte: estat actual, tasques)
