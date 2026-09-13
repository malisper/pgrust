CREATE EXTENSION isn;

-- isn.weak is a Boolean GUC and "isn" a reserved prefix once the library is
-- loaded (_PG_init).
SELECT isn_weak();
SET isn.weak = 'garbage';
SELECT isn_weak();
SET isn.weak = 'yes';
SHOW isn.weak;
SELECT isn_weak(false);
SHOW isn.weak;
SET isn.typo = 'on';
RESET isn.weak;
SHOW isn.weak;
