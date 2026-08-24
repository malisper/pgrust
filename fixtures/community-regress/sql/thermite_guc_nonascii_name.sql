-- Finding (thermite idx 27): non-ASCII custom GUC names never matched their
-- registry key (fold_name re-encoded >=0x80 bytes), so SET then SHOW failed
-- and every SET leaked a placeholder. SET/SHOW must round-trip.
SET "thermite.café" = 'x';
SHOW "thermite.café";
SET "thermite.café" = 'y';
SHOW "thermite.café";
