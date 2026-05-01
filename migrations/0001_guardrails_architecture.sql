-- =============================================================================
-- Migration  : 0001_guardrails_architecture
-- Date       : 2026-04-05
-- Project    : sulugfyxbsxouhvrlwdj  (vademecum)
-- Description:
--   Adds the full guardrails layer for the WhatsApp RAG veterinary bot:
--     1. Three new ENUM types  (chat_intent, risk_level, strike_type)
--     2. Seven new columns on chat_logs  (intent, risk, blocking metadata,
--        idempotency key, classifier bypass flag)
--     3. New table user_strikes  (abuse / rate-limit tracking)
--     4. Indexes for guardrail analytics and strike lookups
--     5. RLS policies on user_strikes
--     6. Replacement body for register_chat_log  (backward-compatible)
--     7. New RPC  get_user_risk_level
--     8. New RPC  add_user_strike
--
--   Safe to re-run: ADD COLUMN uses IF NOT EXISTS, CREATE TYPE uses DO blocks,
--   CREATE TABLE uses IF NOT EXISTS, CREATE INDEX uses IF NOT EXISTS,
--   CREATE OR REPLACE handles RPC replacement.
-- =============================================================================


-- ============================================================
-- 1. ENUM TYPES
--    Postgres < 15 has no IF NOT EXISTS for CREATE TYPE.
--    Use a DO block to check pg_type before creating.
-- ============================================================

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_type
    WHERE typname = 'chat_intent' AND typnamespace = 'public'::regnamespace
  ) THEN
    CREATE TYPE public.chat_intent AS ENUM (
      'vet_query',
      'nonsense',
      'abuse',
      'prompt_injection',
      'out_of_scope',
      'greeting'
    );
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_type
    WHERE typname = 'risk_level' AND typnamespace = 'public'::regnamespace
  ) THEN
    CREATE TYPE public.risk_level AS ENUM (
      'low',
      'medium',
      'high'
    );
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_type
    WHERE typname = 'strike_type' AND typnamespace = 'public'::regnamespace
  ) THEN
    CREATE TYPE public.strike_type AS ENUM (
      'abuse',
      'prompt_injection',
      'nonsense_flood',
      'rate_limit',
      'manual_admin'
    );
  END IF;
END
$$;


-- ============================================================
-- 2. NEW COLUMNS ON chat_logs
--    Every ADD COLUMN uses IF NOT EXISTS (Postgres 9.6+).
-- ============================================================

ALTER TABLE public.chat_logs
  ADD COLUMN IF NOT EXISTS intent_detected      public.chat_intent,
  ADD COLUMN IF NOT EXISTS risk_score           public.risk_level,
  ADD COLUMN IF NOT EXISTS blocked_reason       text,
  ADD COLUMN IF NOT EXISTS should_retrieve      boolean,
  ADD COLUMN IF NOT EXISTS normalized_question  text,
  ADD COLUMN IF NOT EXISTS classifier_bypass    boolean;

-- twilio_message_sid gets its own statement because UNIQUE requires
-- the constraint to be named and we only add it if absent.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name   = 'chat_logs'
      AND column_name  = 'twilio_message_sid'
  ) THEN
    ALTER TABLE public.chat_logs
      ADD COLUMN twilio_message_sid text;

    ALTER TABLE public.chat_logs
      ADD CONSTRAINT chat_logs_twilio_message_sid_key
      UNIQUE (twilio_message_sid);
  END IF;
END
$$;


-- ============================================================
-- 3. TABLE user_strikes
-- ============================================================

CREATE TABLE IF NOT EXISTS public.user_strikes (
  id                    bigint          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  phone_number          text            NOT NULL,
  authorized_number_id  integer         REFERENCES public.authorized_numbers (id)
                                          ON DELETE SET NULL,
  strike_type           public.strike_type NOT NULL,
  reason                text,
  expires_at            timestamptz,
  created_at            timestamptz     NOT NULL DEFAULT now()
);

COMMENT ON TABLE public.user_strikes IS
  'Audit and enforcement log for guardrail violations per WhatsApp number.';

COMMENT ON COLUMN public.user_strikes.expires_at IS
  'NULL means the strike is permanent. Set for time-limited blocks (e.g. rate_limit).';

COMMENT ON COLUMN public.user_strikes.authorized_number_id IS
  'Optional FK to authorized_numbers; NULL when strike is recorded before the '
  'number is registered or for unknown numbers.';


-- ============================================================
-- 4. INDEXES
-- ============================================================

-- Guardrail analytics on chat_logs: filter rows that were actually
-- classified (intent_detected IS NOT NULL) for dashboards / audits.
CREATE INDEX IF NOT EXISTS idx_chat_logs_intent_risk
  ON public.chat_logs (intent_detected, risk_score)
  WHERE intent_detected IS NOT NULL;

-- Fast lookup of recent strikes per phone number (used in get_user_risk_level).
CREATE INDEX IF NOT EXISTS idx_user_strikes_phone_created
  ON public.user_strikes (phone_number, created_at DESC);

-- Lookup non-permanent strikes by phone number (permanent = expires_at IS NULL).
-- The > now() filter cannot be used in index predicates (requires IMMUTABLE).
-- Expiry filtering happens at query time in get_user_risk_level.
CREATE INDEX IF NOT EXISTS idx_user_strikes_phone_active
  ON public.user_strikes (phone_number, expires_at)
  WHERE expires_at IS NOT NULL;

-- Lookup strikes by type (useful for manual_admin queries).
CREATE INDEX IF NOT EXISTS idx_user_strikes_type
  ON public.user_strikes (strike_type, created_at DESC);


-- ============================================================
-- 5. RLS POLICIES FOR user_strikes
--
--    RLS is enabled but zero policies exist on the confirmed tables.
--    user_strikes should be service-role only: no direct anon/user
--    reads or writes. All access goes through SECURITY DEFINER RPCs.
-- ============================================================

ALTER TABLE public.user_strikes ENABLE ROW LEVEL SECURITY;

-- Deny all direct access for non-service roles.
-- The SECURITY DEFINER RPCs bypass RLS and are the sole access path.
-- We create explicit deny-all policies so the intent is documented
-- in pg_policies rather than relying on implicit RLS block.

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename  = 'user_strikes'
      AND policyname = 'deny_all_select'
  ) THEN
    CREATE POLICY deny_all_select ON public.user_strikes
      AS RESTRICTIVE FOR SELECT
      USING (false);
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename  = 'user_strikes'
      AND policyname = 'deny_all_insert'
  ) THEN
    CREATE POLICY deny_all_insert ON public.user_strikes
      AS RESTRICTIVE FOR INSERT
      WITH CHECK (false);
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename  = 'user_strikes'
      AND policyname = 'deny_all_update'
  ) THEN
    CREATE POLICY deny_all_update ON public.user_strikes
      AS RESTRICTIVE FOR UPDATE
      USING (false);
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename  = 'user_strikes'
      AND policyname = 'deny_all_delete'
  ) THEN
    CREATE POLICY deny_all_delete ON public.user_strikes
      AS RESTRICTIVE FOR DELETE
      USING (false);
  END IF;
END
$$;


-- ============================================================
-- 6. register_chat_log  (full replacement, backward-compatible)
--
--    Original signature (confirmed live):
--      register_chat_log(
--        p_phone_number, p_input_type, p_message_in, p_message_out,
--        p_provider, p_tokens_used, p_language, p_sources,
--        p_tokens_audio_model, p_tokens_image_model,
--        p_tokens_lang_model, p_tokens_response_model
--      )
--
--    New optional params (all DEFAULT NULL — existing callers unaffected):
--      p_intent_detected, p_risk_score, p_blocked_reason,
--      p_should_retrieve, p_normalized_question,
--      p_twilio_message_sid, p_classifier_bypass
--
--    Idempotency: if p_twilio_message_sid is supplied and already exists,
--    the function returns the existing row id without inserting a duplicate.
-- ============================================================

-- Drop the old 12-param signature so CREATE OR REPLACE is unambiguous.
-- The new 19-param version is backward-compatible via DEFAULT NULL.
DROP FUNCTION IF EXISTS public.register_chat_log(
  text, text, text, text, text, integer, text, jsonb,
  integer, integer, integer, integer
);

CREATE OR REPLACE FUNCTION public.register_chat_log(
  p_phone_number           text,
  p_input_type             text,
  p_message_in             text,
  p_message_out            text,
  p_provider               text,
  p_tokens_used            integer,
  p_language               text,
  p_sources                jsonb,
  p_tokens_audio_model     integer,
  p_tokens_image_model     integer,
  p_tokens_lang_model      integer,
  p_tokens_response_model  integer,
  -- New guardrail params — all nullable so existing callers are unaffected
  p_intent_detected        public.chat_intent  DEFAULT NULL,
  p_risk_score             public.risk_level   DEFAULT NULL,
  p_blocked_reason         text                DEFAULT NULL,
  p_should_retrieve        boolean             DEFAULT NULL,
  p_normalized_question    text                DEFAULT NULL,
  p_twilio_message_sid     text                DEFAULT NULL,
  p_classifier_bypass      boolean             DEFAULT NULL
)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_user_id     uuid;
  v_log_id      uuid;
  v_session_id  uuid;
  v_existing_id uuid;
BEGIN
  -- ---- Idempotency guard -----------------------------------------------
  -- If a Twilio SID is provided and already recorded, return early.
  -- This prevents double-inserts from Twilio retries.
  IF p_twilio_message_sid IS NOT NULL THEN
    SELECT id INTO v_existing_id
    FROM public.chat_logs
    WHERE twilio_message_sid = p_twilio_message_sid
    LIMIT 1;

    IF v_existing_id IS NOT NULL THEN
      RETURN v_existing_id;
    END IF;
  END IF;

  -- ---- Resolve user -------------------------------------------------------
  SELECT u.id INTO v_user_id
  FROM public.authorized_numbers an
  JOIN public.users u ON u.id = an.user_id
  WHERE an.phone_number = p_phone_number
    AND an.is_active = true
  LIMIT 1;

  IF v_user_id IS NULL THEN
    RAISE EXCEPTION 'No active authorized_number found for phone %', p_phone_number;
  END IF;

  -- ---- Resolve or create session -------------------------------------------
  -- Session is keyed by user + a rolling window anchored to conversation_reset_at.
  -- Reuse the most recent session uuid if it is newer than conversation_reset_at;
  -- otherwise mint a fresh one.
  SELECT cl.session_id INTO v_session_id
  FROM public.chat_logs cl
  JOIN public.users u ON u.id = cl.user_id
  WHERE cl.user_id = v_user_id
    AND (
      u.conversation_reset_at IS NULL
      OR cl.created_at > u.conversation_reset_at
    )
  ORDER BY cl.created_at DESC
  LIMIT 1;

  IF v_session_id IS NULL THEN
    v_session_id := gen_random_uuid();
  END IF;

  -- ---- Insert -------------------------------------------------------------
  INSERT INTO public.chat_logs (
    user_id,
    whatsapp_input_format,
    message_in,
    message_out,
    provider,
    tokens_used,
    language,
    sources,
    session_id,
    tokens_audio_model,
    tokens_image_model,
    tokens_lang_model,
    tokens_response_model,
    -- guardrail columns
    intent_detected,
    risk_score,
    blocked_reason,
    should_retrieve,
    normalized_question,
    twilio_message_sid,
    classifier_bypass
  )
  VALUES (
    v_user_id,
    p_input_type,
    p_message_in,
    p_message_out,
    p_provider,
    p_tokens_used,
    p_language,
    p_sources,
    v_session_id,
    p_tokens_audio_model,
    p_tokens_image_model,
    p_tokens_lang_model,
    p_tokens_response_model,
    p_intent_detected,
    p_risk_score,
    p_blocked_reason,
    p_should_retrieve,
    p_normalized_question,
    p_twilio_message_sid,
    p_classifier_bypass
  )
  RETURNING id INTO v_log_id;

  RETURN v_log_id;
END;
$$;

COMMENT ON FUNCTION public.register_chat_log IS
  'Inserts a chat log row for a WhatsApp message. '
  'Idempotent when p_twilio_message_sid is provided. '
  'New guardrail params (intent_detected … classifier_bypass) are all '
  'optional with DEFAULT NULL — existing n8n callers require no changes.';


-- ============================================================
-- 7. get_user_risk_level
--
--    Returns a jsonb summary:
--      { strikes_last_24h, strikes_last_7d, risk_level }
--    Risk level derivation (conservative defaults — tune as needed):
--      high   >= 3 strikes in last 24 h
--      medium >= 2 strikes in last 24 h  OR  >= 5 in last 7 d
--      low    everything else
-- ============================================================

CREATE OR REPLACE FUNCTION public.get_user_risk_level(
  p_phone_number text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_strikes_24h  integer;
  v_strikes_7d   integer;
  v_risk         public.risk_level;
BEGIN
  SELECT
    COUNT(*) FILTER (WHERE created_at >= now() - interval '24 hours'),
    COUNT(*) FILTER (WHERE created_at >= now() - interval '7 days')
  INTO v_strikes_24h, v_strikes_7d
  FROM public.user_strikes
  WHERE phone_number = p_phone_number
    AND (expires_at IS NULL OR expires_at > now());

  -- Risk classification
  IF v_strikes_24h >= 3 THEN
    v_risk := 'high';
  ELSIF v_strikes_24h >= 2 OR v_strikes_7d >= 5 THEN
    v_risk := 'medium';
  ELSE
    v_risk := 'low';
  END IF;

  RETURN jsonb_build_object(
    'phone_number',    p_phone_number,
    'strikes_last_24h', v_strikes_24h,
    'strikes_last_7d',  v_strikes_7d,
    'risk_level',       v_risk::text
  );
END;
$$;

COMMENT ON FUNCTION public.get_user_risk_level IS
  'Returns strike counts (24 h, 7 d) and a derived risk_level for a phone '
  'number. Only non-expired strikes are counted. '
  'Thresholds: high >= 3/24h; medium >= 2/24h or >= 5/7d; else low.';


-- ============================================================
-- 8. add_user_strike
--
--    Inserts a strike and returns the new row id.
--    Resolves authorized_number_id automatically when the number
--    exists in authorized_numbers; leaves it NULL otherwise so
--    unknown/unregistered numbers can still be tracked.
-- ============================================================

CREATE OR REPLACE FUNCTION public.add_user_strike(
  p_phone_number  text,
  p_strike_type   public.strike_type,
  p_reason        text        DEFAULT NULL,
  p_expires_at    timestamptz DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_authorized_number_id  integer;
  v_strike_id             bigint;
BEGIN
  -- Try to link to authorized_numbers (best-effort, not mandatory).
  SELECT id INTO v_authorized_number_id
  FROM public.authorized_numbers
  WHERE phone_number = p_phone_number
  LIMIT 1;

  INSERT INTO public.user_strikes (
    phone_number,
    authorized_number_id,
    strike_type,
    reason,
    expires_at
  )
  VALUES (
    p_phone_number,
    v_authorized_number_id,
    p_strike_type,
    p_reason,
    p_expires_at
  )
  RETURNING id INTO v_strike_id;

  RETURN v_strike_id;
END;
$$;

COMMENT ON FUNCTION public.add_user_strike IS
  'Records a guardrail violation for a phone number. '
  'Automatically links to authorized_numbers when the number is known. '
  'p_expires_at NULL means the strike is permanent. '
  'Returns the new user_strikes.id.';


-- ============================================================
-- END OF MIGRATION 0001_guardrails_architecture
-- ============================================================
