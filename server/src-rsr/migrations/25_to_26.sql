CREATE TABLE hdb_catalog.hdb_action
(
  action_name TEXT PRIMARY KEY,
  action_defn JSONB NOT NULL,
  comment TEXT NULL,
  is_system_defined boolean default false
);

CREATE TABLE hdb_catalog.hdb_action_permission
(
  action_name TEXT NOT NULL,
  role_name TEXT NOT NULL,
  definition JSONB NOT NULL,
  comment    TEXT NULL,

  PRIMARY KEY (action_name, role_name),
  FOREIGN KEY (action_name) REFERENCES hdb_catalog.hdb_action(action_name) ON UPDATE CASCADE
);

CREATE TABLE hdb_catalog.hdb_action_log
(
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  -- we deliberately do not reference the action name
  -- because sometimes we may want to retain history
  -- when after dropping the action
  action_name TEXT,
  input_payload JSONB NOT NULL,
  session_variables JSONB NOT NULL,
  response_payload JSONB NULL,

  created_at timestamptz NOT NULL default now(),
  response_received_at timestamptz NULL,
  status text NOT NULL,
  CHECK (status IN ('created', 'processing', 'completed', 'error'))
);

CREATE TABLE hdb_catalog.hdb_custom_graphql_types
(
  custom_types jsonb NOT NULL
);
