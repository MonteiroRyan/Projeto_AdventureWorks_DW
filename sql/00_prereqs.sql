-- Criação de schema e tipos auxiliares
CREATE SCHEMA IF NOT EXISTS dw;

-- Tabela de controle de execução (watermarks simples)
CREATE TABLE IF NOT EXISTS dw.etl_run_control (
  pipeline_name text PRIMARY KEY,
  last_watermark_value text,
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- Índice útil para consultas de controle
CREATE INDEX IF NOT EXISTS ix_etl_run_control_updated_at ON dw.etl_run_control(updated_at);