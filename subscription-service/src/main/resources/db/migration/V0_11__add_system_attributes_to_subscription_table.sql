ALTER TABLE subscription
ADD created_at TIMESTAMPTZ NOT NULL default TIMESTAMP 'epoch',
ADD modified_at TIMESTAMPTZ;