//! C-parity witnesses for audit batch b033-contrib-pgcrypto-1 (contrib/pgcrypto,
//! oracle = live PostgreSQL 18.6 pgcrypto, pair run 2026-09-05). Each test
//! names the C site whose observable behaviour it locks. The crafted keys
//! derive from the C regress `elg1024` keytbl row (sql/pgp-pubkey-decrypt.sql):
//! DSA main key + Elgamal encryption subkey, unprotected secret material.
//! The SQL twin is scripts/pgcrypto-conformance-e2e.sh.

use super::*;
use crate::pgp::consts;

const CTL_PUB: &str = "c6c0e20442c82148110400a9e34d4bea35eb2c1dbc737600e2c3373892a7c4cd464fd613916c84dd5cad7bae47f1653d\
    b463c15fb38cb9a5f978667ce0b1541af68fd24eda9df9977039023b73fd1a02feae8fdd4b63ecfef0a140f66a1dac44\
    c6928352de3b07b0c675126b0bc0d19c8478c1b4b2400ea66b74b5c85a82e692614acfa6a9ff5e1313a38b00a09edb61\
    925471218c37e76657f4ebfda21b7fa7f103ff55e2e0f4beb645d5bf2230e5a65f6bad5b4fa5ef0e4a0f234023dc6139\
    626087265280fc8844afc40938b57efb9544bf897a290d7f0c5a09ce835daf500626e9204dce97fd67104737d0b5489c\
    3c74267b9a005a81dae9b55064ec2f6c987798e5c3abf4e4fca176da0a3c99a338e2f327e1bbeff9205bb25be260b88b\
    8d927503ff495c5d5eb0dabe5fd1de849caa1c753b2c411ffdc93acc2ee8d9eaccdff2d9959b8b34f0f672fde82f736f\
    b1a12071349997ef1798ef292e6ce32886fe862ef4a95c77b7622132a6dbe01fdd318d4293ada03c129af35fd6c22eef\
    22c10140b12dc81986988eb8629ce844d15e69a17d258fac00aa913aa7ba74b8c9eef79b40cec04d0442c8214a100400\
    87948d6db24c02c27eb106dc584cdd92ef00758079cd8ed0c9ff443af9f4837f5bcc0361c6699bea06d19503748a8ca6\
    943c2b69329401fb8266035c83fd0fb311466fd9c372f8c857ca9d5699ead4fbb3305b816e6188eb07a0ecf8f4d5d94f\
    88ed30b7594b5fe4ae6e4b6a6d8c48fa1df573744366ac63f8a020c51f18346300030603fb042cd877b5679fa9e0e4a0\
    30446237317469c6546267347beffdea07cbd1fb7720ff92485a3a84406b90a56164aa0f49246be4aa4d684a1b85dc6c\
    9ca8d46b0feaca868570d3336f8b0593cc2ae54942911fa1ef6bba86ff05ba578257c489513d54d898ed957251d8f7b2\
    f627e1f2b65fbb9ed46ddc1d1dad2f8967380e2961";
const CTL_SEC: &str = "c5c0fb0442c82148110400a9e34d4bea35eb2c1dbc737600e2c3373892a7c4cd464fd613916c84dd5cad7bae47f1653d\
    b463c15fb38cb9a5f978667ce0b1541af68fd24eda9df9977039023b73fd1a02feae8fdd4b63ecfef0a140f66a1dac44\
    c6928352de3b07b0c675126b0bc0d19c8478c1b4b2400ea66b74b5c85a82e692614acfa6a9ff5e1313a38b00a09edb61\
    925471218c37e76657f4ebfda21b7fa7f103ff55e2e0f4beb645d5bf2230e5a65f6bad5b4fa5ef0e4a0f234023dc6139\
    626087265280fc8844afc40938b57efb9544bf897a290d7f0c5a09ce835daf500626e9204dce97fd67104737d0b5489c\
    3c74267b9a005a81dae9b55064ec2f6c987798e5c3abf4e4fca176da0a3c99a338e2f327e1bbeff9205bb25be260b88b\
    8d927503ff495c5d5eb0dabe5fd1de849caa1c753b2c411ffdc93acc2ee8d9eaccdff2d9959b8b34f0f672fde82f736f\
    b1a12071349997ef1798ef292e6ce32886fe862ef4a95c77b7622132a6dbe01fdd318d4293ada03c129af35fd6c22eef\
    22c10140b12dc81986988eb8629ce844d15e69a17d258fac00aa913aa7ba74b8c9eef79b4000009e3e22e2cb7eb3e0ba\
    58a4c80dc2f5232d126aaf250a9ec7c0710442c8214a10040087948d6db24c02c27eb106dc584cdd92ef00758079cd8e\
    d0c9ff443af9f4837f5bcc0361c6699bea06d19503748a8ca6943c2b69329401fb8266035c83fd0fb311466fd9c372f8\
    c857ca9d5699ead4fbb3305b816e6188eb07a0ecf8f4d5d94f88ed30b7594b5fe4ae6e4b6a6d8c48fa1df573744366ac\
    63f8a020c51f18346300030603fb042cd877b5679fa9e0e4a030446237317469c6546267347beffdea07cbd1fb7720ff\
    92485a3a84406b90a56164aa0f49246be4aa4d684a1b85dc6c9ca8d46b0feaca868570d3336f8b0593cc2ae54942911f\
    a1ef6bba86ff05ba578257c489513d54d898ed957251d8f7b2f627e1f2b65fbb9ed46ddc1d1dad2f8967380e29610000\
    f74df4138bb664a39f241a5433e10c9bd566b068da419578b5e07447fab75bcb1121";
const MSG1: &str = "85010e03d936cf64bb73f466100400815596d0378bad9fb5b615891813e9d964452fd1de36aa961db28909725bcf5532\
    7da518ef2c55bc6e44bad9a8f893226379a0db7fc38155571d066c4d6a46bc633a7638143462948d90db5ba36883b32b\
    7bbf08cb06adb423a68143fdbc8c308373958ee0c237f9d58ab1901675e924173c0f35aa0cc591583cb533466c2bb003\
    ff6253a2c485c5476938dd032ad218dc62f39a1e8d937895d20f15809294053d6b8425ee3038f7b279bffc4326ee14d8\
    3e59ce6eae26221833f0daa9ae6868a319d4d0a6a9a1f6f73f7c021cfa54d78cf185763c88a3bfdc9841ab6b85731e17\
    b81330916e0074dc58fe6cc96442ed7932fc4ebd2cecf21293ea306f8511a46de7d23b0173409550bc6b8e1e448e4770\
    c02335f7927af8a6d0c4e3854346f7b8e56fbf874481dfe14444423aae443da1619477a0c9827b60ef38daf9d20c";
const K1_LEADZERO_PUB: &str = "c6c0e20442c82148110400a9e34d4bea35eb2c1dbc737600e2c3373892a7c4cd464fd613916c84dd5cad7bae47f1653d\
    b463c15fb38cb9a5f978667ce0b1541af68fd24eda9df9977039023b73fd1a02feae8fdd4b63ecfef0a140f66a1dac44\
    c6928352de3b07b0c675126b0bc0d19c8478c1b4b2400ea66b74b5c85a82e692614acfa6a9ff5e1313a38b00a09edb61\
    925471218c37e76657f4ebfda21b7fa7f103ff55e2e0f4beb645d5bf2230e5a65f6bad5b4fa5ef0e4a0f234023dc6139\
    626087265280fc8844afc40938b57efb9544bf897a290d7f0c5a09ce835daf500626e9204dce97fd67104737d0b5489c\
    3c74267b9a005a81dae9b55064ec2f6c987798e5c3abf4e4fca176da0a3c99a338e2f327e1bbeff9205bb25be260b88b\
    8d927503ff495c5d5eb0dabe5fd1de849caa1c753b2c411ffdc93acc2ee8d9eaccdff2d9959b8b34f0f672fde82f736f\
    b1a12071349997ef1798ef292e6ce32886fe862ef4a95c77b7622132a6dbe01fdd318d4293ada03c129af35fd6c22eef\
    22c10140b12dc81986988eb8629ce844d15e69a17d258fac00aa913aa7ba74b8c9eef79b40cec04d0442c8214a100400\
    87948d6db24c02c27eb106dc584cdd92ef00758079cd8ed0c9ff443af9f4837f5bcc0361c6699bea06d19503748a8ca6\
    943c2b69329401fb8266035c83fd0fb311466fd9c372f8c857ca9d5699ead4fbb3305b816e6188eb07a0ecf8f4d5d94f\
    88ed30b7594b5fe4ae6e4b6a6d8c48fa1df573744366ac63f8a020c51f1834630008060400042cd877b5679fa9e0e4a0\
    30446237317469c6546267347beffdea07cbd1fb7720ff92485a3a84406b90a56164aa0f49246be4aa4d684a1b85dc6c\
    9ca8d46b0feaca868570d3336f8b0593cc2ae54942911fa1ef6bba86ff05ba578257c489513d54d898ed957251d8f7b2\
    f627e1f2b65fbb9ed46ddc1d1dad2f8967380e2961";
const K2_UNKNOWN_ALGO_PUB: &str = "c6c0e20442c82148110400a9e34d4bea35eb2c1dbc737600e2c3373892a7c4cd464fd613916c84dd5cad7bae47f1653d\
    b463c15fb38cb9a5f978667ce0b1541af68fd24eda9df9977039023b73fd1a02feae8fdd4b63ecfef0a140f66a1dac44\
    c6928352de3b07b0c675126b0bc0d19c8478c1b4b2400ea66b74b5c85a82e692614acfa6a9ff5e1313a38b00a09edb61\
    925471218c37e76657f4ebfda21b7fa7f103ff55e2e0f4beb645d5bf2230e5a65f6bad5b4fa5ef0e4a0f234023dc6139\
    626087265280fc8844afc40938b57efb9544bf897a290d7f0c5a09ce835daf500626e9204dce97fd67104737d0b5489c\
    3c74267b9a005a81dae9b55064ec2f6c987798e5c3abf4e4fca176da0a3c99a338e2f327e1bbeff9205bb25be260b88b\
    8d927503ff495c5d5eb0dabe5fd1de849caa1c753b2c411ffdc93acc2ee8d9eaccdff2d9959b8b34f0f672fde82f736f\
    b1a12071349997ef1798ef292e6ce32886fe862ef4a95c77b7622132a6dbe01fdd318d4293ada03c129af35fd6c22eef\
    22c10140b12dc81986988eb8629ce844d15e69a17d258fac00aa913aa7ba74b8c9eef79b40ce060442c8214a63cec04d\
    0442c8214a10040087948d6db24c02c27eb106dc584cdd92ef00758079cd8ed0c9ff443af9f4837f5bcc0361c6699bea\
    06d19503748a8ca6943c2b69329401fb8266035c83fd0fb311466fd9c372f8c857ca9d5699ead4fbb3305b816e6188eb\
    07a0ecf8f4d5d94f88ed30b7594b5fe4ae6e4b6a6d8c48fa1df573744366ac63f8a020c51f18346300030603fb042cd8\
    77b5679fa9e0e4a030446237317469c6546267347beffdea07cbd1fb7720ff92485a3a84406b90a56164aa0f49246be4\
    aa4d684a1b85dc6c9ca8d46b0feaca868570d3336f8b0593cc2ae54942911fa1ef6bba86ff05ba578257c489513d54d8\
    98ed957251d8f7b2f627e1f2b65fbb9ed46ddc1d1dad2f8967380e2961";
const K3_V3_TRUNC_PUB: &str = "c6c0e20442c82148110400a9e34d4bea35eb2c1dbc737600e2c3373892a7c4cd464fd613916c84dd5cad7bae47f1653d\
    b463c15fb38cb9a5f978667ce0b1541af68fd24eda9df9977039023b73fd1a02feae8fdd4b63ecfef0a140f66a1dac44\
    c6928352de3b07b0c675126b0bc0d19c8478c1b4b2400ea66b74b5c85a82e692614acfa6a9ff5e1313a38b00a09edb61\
    925471218c37e76657f4ebfda21b7fa7f103ff55e2e0f4beb645d5bf2230e5a65f6bad5b4fa5ef0e4a0f234023dc6139\
    626087265280fc8844afc40938b57efb9544bf897a290d7f0c5a09ce835daf500626e9204dce97fd67104737d0b5489c\
    3c74267b9a005a81dae9b55064ec2f6c987798e5c3abf4e4fca176da0a3c99a338e2f327e1bbeff9205bb25be260b88b\
    8d927503ff495c5d5eb0dabe5fd1de849caa1c753b2c411ffdc93acc2ee8d9eaccdff2d9959b8b34f0f672fde82f736f\
    b1a12071349997ef1798ef292e6ce32886fe862ef4a95c77b7622132a6dbe01fdd318d4293ada03c129af35fd6c22eef\
    22c10140b12dc81986988eb8629ce844d15e69a17d258fac00aa913aa7ba74b8c9eef79b40cec04d0442c8214a100400\
    87948d6db24c02c27eb106dc584cdd92ef00758079cd8ed0c9ff443af9f4837f5bcc0361c6699bea06d19503748a8ca6\
    943c2b69329401fb8266035c83fd0fb311466fd9c372f8c857ca9d5699ead4fbb3305b816e6188eb07a0ecf8f4d5d94f\
    88ed30b7594b5fe4ae6e4b6a6d8c48fa1df573744366ac63f8a020c51f18346300030603fb042cd877b5679fa9e0e4a0\
    30446237317469c6546267347beffdea07cbd1fb7720ff92485a3a84406b90a56164aa0f49246be4aa4d684a1b85dc6c\
    9ca8d46b0feaca868570d3336f8b0593cc2ae54942911fa1ef6bba86ff05ba578257c489513d54d898ed957251d8f7b2\
    f627e1f2b65fbb9ed46ddc1d1dad2f8967380e2961ce020300";
const K4_TRAILING_SEC: &str = "c5c0fb0442c82148110400a9e34d4bea35eb2c1dbc737600e2c3373892a7c4cd464fd613916c84dd5cad7bae47f1653d\
    b463c15fb38cb9a5f978667ce0b1541af68fd24eda9df9977039023b73fd1a02feae8fdd4b63ecfef0a140f66a1dac44\
    c6928352de3b07b0c675126b0bc0d19c8478c1b4b2400ea66b74b5c85a82e692614acfa6a9ff5e1313a38b00a09edb61\
    925471218c37e76657f4ebfda21b7fa7f103ff55e2e0f4beb645d5bf2230e5a65f6bad5b4fa5ef0e4a0f234023dc6139\
    626087265280fc8844afc40938b57efb9544bf897a290d7f0c5a09ce835daf500626e9204dce97fd67104737d0b5489c\
    3c74267b9a005a81dae9b55064ec2f6c987798e5c3abf4e4fca176da0a3c99a338e2f327e1bbeff9205bb25be260b88b\
    8d927503ff495c5d5eb0dabe5fd1de849caa1c753b2c411ffdc93acc2ee8d9eaccdff2d9959b8b34f0f672fde82f736f\
    b1a12071349997ef1798ef292e6ce32886fe862ef4a95c77b7622132a6dbe01fdd318d4293ada03c129af35fd6c22eef\
    22c10140b12dc81986988eb8629ce844d15e69a17d258fac00aa913aa7ba74b8c9eef79b4000009e3e22e2cb7eb3e0ba\
    58a4c80dc2f5232d126aaf250a9ec7c0740442c8214a10040087948d6db24c02c27eb106dc584cdd92ef00758079cd8e\
    d0c9ff443af9f4837f5bcc0361c6699bea06d19503748a8ca6943c2b69329401fb8266035c83fd0fb311466fd9c372f8\
    c857ca9d5699ead4fbb3305b816e6188eb07a0ecf8f4d5d94f88ed30b7594b5fe4ae6e4b6a6d8c48fa1df573744366ac\
    63f8a020c51f18346300030603fb042cd877b5679fa9e0e4a030446237317469c6546267347beffdea07cbd1fb7720ff\
    92485a3a84406b90a56164aa0f49246be4aa4d684a1b85dc6c9ca8d46b0feaca868570d3336f8b0593cc2ae54942911f\
    a1ef6bba86ff05ba578257c489513d54d898ed957251d8f7b2f627e1f2b65fbb9ed46ddc1d1dad2f8967380e29610000\
    f74df4138bb664a39f241a5433e10c9bd566b068da419578b5e07447fab75bcb1121010203";
const K5_BADCKSUM_SEC: &str = "c5c0fb0442c82148110400a9e34d4bea35eb2c1dbc737600e2c3373892a7c4cd464fd613916c84dd5cad7bae47f1653d\
    b463c15fb38cb9a5f978667ce0b1541af68fd24eda9df9977039023b73fd1a02feae8fdd4b63ecfef0a140f66a1dac44\
    c6928352de3b07b0c675126b0bc0d19c8478c1b4b2400ea66b74b5c85a82e692614acfa6a9ff5e1313a38b00a09edb61\
    925471218c37e76657f4ebfda21b7fa7f103ff55e2e0f4beb645d5bf2230e5a65f6bad5b4fa5ef0e4a0f234023dc6139\
    626087265280fc8844afc40938b57efb9544bf897a290d7f0c5a09ce835daf500626e9204dce97fd67104737d0b5489c\
    3c74267b9a005a81dae9b55064ec2f6c987798e5c3abf4e4fca176da0a3c99a338e2f327e1bbeff9205bb25be260b88b\
    8d927503ff495c5d5eb0dabe5fd1de849caa1c753b2c411ffdc93acc2ee8d9eaccdff2d9959b8b34f0f672fde82f736f\
    b1a12071349997ef1798ef292e6ce32886fe862ef4a95c77b7622132a6dbe01fdd318d4293ada03c129af35fd6c22eef\
    22c10140b12dc81986988eb8629ce844d15e69a17d258fac00aa913aa7ba74b8c9eef79b4000009e3e22e2cb7eb3e0ba\
    58a4c80dc2f5232d126aaf250a9ec7c0710442c8214a10040087948d6db24c02c27eb106dc584cdd92ef00758079cd8e\
    d0c9ff443af9f4837f5bcc0361c6699bea06d19503748a8ca6943c2b69329401fb8266035c83fd0fb311466fd9c372f8\
    c857ca9d5699ead4fbb3305b816e6188eb07a0ecf8f4d5d94f88ed30b7594b5fe4ae6e4b6a6d8c48fa1df573744366ac\
    63f8a020c51f18346300030603fb042cd877b5679fa9e0e4a030446237317469c6546267347beffdea07cbd1fb7720ff\
    92485a3a84406b90a56164aa0f49246be4aa4d684a1b85dc6c9ca8d46b0feaca868570d3336f8b0593cc2ae54942911f\
    a1ef6bba86ff05ba578257c489513d54d898ed957251d8f7b2f627e1f2b65fbb9ed46ddc1d1dad2f8967380e29610000\
    f74df4138bb664a39f241a5433e10c9bd566b068da419578b5e07447fab75bcb1120";
const K6_DSA_SIGNSUB_SEC: &str = "c5c0fb0442c82148110400a9e34d4bea35eb2c1dbc737600e2c3373892a7c4cd464fd613916c84dd5cad7bae47f1653d\
    b463c15fb38cb9a5f978667ce0b1541af68fd24eda9df9977039023b73fd1a02feae8fdd4b63ecfef0a140f66a1dac44\
    c6928352de3b07b0c675126b0bc0d19c8478c1b4b2400ea66b74b5c85a82e692614acfa6a9ff5e1313a38b00a09edb61\
    925471218c37e76657f4ebfda21b7fa7f103ff55e2e0f4beb645d5bf2230e5a65f6bad5b4fa5ef0e4a0f234023dc6139\
    626087265280fc8844afc40938b57efb9544bf897a290d7f0c5a09ce835daf500626e9204dce97fd67104737d0b5489c\
    3c74267b9a005a81dae9b55064ec2f6c987798e5c3abf4e4fca176da0a3c99a338e2f327e1bbeff9205bb25be260b88b\
    8d927503ff495c5d5eb0dabe5fd1de849caa1c753b2c411ffdc93acc2ee8d9eaccdff2d9959b8b34f0f672fde82f736f\
    b1a12071349997ef1798ef292e6ce32886fe862ef4a95c77b7622132a6dbe01fdd318d4293ada03c129af35fd6c22eef\
    22c10140b12dc81986988eb8629ce844d15e69a17d258fac00aa913aa7ba74b8c9eef79b4000009e3e22e2cb7eb3e0ba\
    58a4c80dc2f5232d126aaf250a9ec7c0fb0442c82148110400a9e34d4bea35eb2c1dbc737600e2c3373892a7c4cd464f\
    d613916c84dd5cad7bae47f1653db463c15fb38cb9a5f978667ce0b1541af68fd24eda9df9977039023b73fd1a02feae\
    8fdd4b63ecfef0a140f66a1dac44c6928352de3b07b0c675126b0bc0d19c8478c1b4b2400ea66b74b5c85a82e692614a\
    cfa6a9ff5e1313a38b00a09edb61925471218c37e76657f4ebfda21b7fa7f103ff55e2e0f4beb645d5bf2230e5a65f6b\
    ad5b4fa5ef0e4a0f234023dc6139626087265280fc8844afc40938b57efb9544bf897a290d7f0c5a09ce835daf500626\
    e9204dce97fd67104737d0b5489c3c74267b9a005a81dae9b55064ec2f6c987798e5c3abf4e4fca176da0a3c99a338e2\
    f327e1bbeff9205bb25be260b88b8d927503ff495c5d5eb0dabe5fd1de849caa1c753b2c411ffdc93acc2ee8d9eaccdf\
    f2d9959b8b34f0f672fde82f736fb1a12071349997ef1798ef292e6ce32886fe862ef4a95c77b7622132a6dbe01fdd31\
    8d4293ada03c129af35fd6c22eef22c10140b12dc81986988eb8629ce844d15e69a17d258fac00aa913aa7ba74b8c9ee\
    f79b4000009e3e22e2cb7eb3e0ba58a4c80dc2f5232d126aaf250a9ec7c0710442c8214a10040087948d6db24c02c27e\
    b106dc584cdd92ef00758079cd8ed0c9ff443af9f4837f5bcc0361c6699bea06d19503748a8ca6943c2b69329401fb82\
    66035c83fd0fb311466fd9c372f8c857ca9d5699ead4fbb3305b816e6188eb07a0ecf8f4d5d94f88ed30b7594b5fe4ae\
    6e4b6a6d8c48fa1df573744366ac63f8a020c51f18346300030603fb042cd877b5679fa9e0e4a030446237317469c654\
    6267347beffdea07cbd1fb7720ff92485a3a84406b90a56164aa0f49246be4aa4d684a1b85dc6c9ca8d46b0feaca8685\
    70d3336f8b0593cc2ae54942911fa1ef6bba86ff05ba578257c489513d54d898ed957251d8f7b2f627e1f2b65fbb9ed4\
    6ddc1d1dad2f8967380e29610000f74df4138bb664a39f241a5433e10c9bd566b068da419578b5e07447fab75bcb1121";
const K6B_RSA_SIGNSUB_SEC: &str = "c5c0fb0442c82148110400a9e34d4bea35eb2c1dbc737600e2c3373892a7c4cd464fd613916c84dd5cad7bae47f1653d\
    b463c15fb38cb9a5f978667ce0b1541af68fd24eda9df9977039023b73fd1a02feae8fdd4b63ecfef0a140f66a1dac44\
    c6928352de3b07b0c675126b0bc0d19c8478c1b4b2400ea66b74b5c85a82e692614acfa6a9ff5e1313a38b00a09edb61\
    925471218c37e76657f4ebfda21b7fa7f103ff55e2e0f4beb645d5bf2230e5a65f6bad5b4fa5ef0e4a0f234023dc6139\
    626087265280fc8844afc40938b57efb9544bf897a290d7f0c5a09ce835daf500626e9204dce97fd67104737d0b5489c\
    3c74267b9a005a81dae9b55064ec2f6c987798e5c3abf4e4fca176da0a3c99a338e2f327e1bbeff9205bb25be260b88b\
    8d927503ff495c5d5eb0dabe5fd1de849caa1c753b2c411ffdc93acc2ee8d9eaccdff2d9959b8b34f0f672fde82f736f\
    b1a12071349997ef1798ef292e6ce32886fe862ef4a95c77b7622132a6dbe01fdd318d4293ada03c129af35fd6c22eef\
    22c10140b12dc81986988eb8629ce844d15e69a17d258fac00aa913aa7ba74b8c9eef79b4000009e3e22e2cb7eb3e0ba\
    58a4c80dc2f5232d126aaf250a9ec7c2d60442c825b1030800c021db5ca0b02641f97ca5bef7a4f46e118c7ca96f6494\
    b7c2186e40e0e50e8b4d091cdc0f65a0017b53c6ce3a1e5eb80cd4c4d0661a19bb555d83e28323a5c0b62cbd3fa6cb83\
    b9edb1792c0f8c01a082ad40a736e79a5dba4c538aefb1789246daaf1964f3a9f7b095f5f6d0f6f3c2c209ee4ac0c2a1\
    09626d98a40d53c0c954efa7b271e6bcfcc97b25e2cc6f69e3193eaa853ed03f10d790ddca6cc91d414174afa19a47d4\
    5ab46fc19c65311d5960ffc3afca302442f139a011dfb3ddef2422c0b0150f934876c2731456760644ee0137d6c2471d\
    592b6655ed2d4f0c3d75aab25573474717fb20bc8fa6d723afc1a99d92bd19dd270bae9d102be2e9bb0006290007ff41\
    9b2baf3d1cedd816c06a8c229c3a7cffbf94b6bbe3e7dad821515ad628c0488ab1f0e0ff037b51ad3bd37e9d3613daf1\
    a64fb14e924739b93f26ca39445e0fda4db741712645f48f9a4259f8d22604db735ac276a67fa32064fd4ea942d7ffff\
    37f0dcf6c397b23c4c2e863addb0e4d81c5ddb161cfecb0263dcb4e554cb316c7a77af4743054c790c6e6288d47a459b\
    c5cdfded4597ea88003f8e6a04c5e41df656fa6826d921e67c6722e4e8349b1af48cbb17d30cd827853b24cc8a131c8d\
    7281054680628175d798ab06faaeecdfea5a6cea2e0f32c41679ea687a99e085dfeebb540db2edf2fcae1c3642eaa02e\
    0d4916528054bfbd72b092ae7fff190400d644fb172b56e8134e7df6e5158664ac97abde2728443a9afe58e01ffc9569\
    6a154f58a0cb413d5f360eeccc0fdaeb23feb7ac2f2d0d0b5a82b61939d5c67fc2d868a0c97cf1ee566f8979158c0111\
    a73b9ae4814e8e16f1c007d611634d62fa5c01af133197dccb7da67f2e6069502d9021268342014d843826cdd4f7210e\
    fb0400e58d2987acb94564f3eb104add0d0cd85df415ad4d64c790265bd5d282dc94dfafd3ea91304bad615b62390eb6\
    f74f016ec50da5d75d97dd1f58c759fc0fdd0f88eacaa049a5a9092e1d0409ecdff455b4eedccd583223099ad38888bc\
    9a9d7ab62b7ef914abde4a7f03f657cc17921acf3cfc72ae0e72df0425a58da36f94410400b5ed22b59acf8306d724e2\
    fa5d8e9dd2277d926f20bf0d4df4f3d435da3ef84bb65dbb5a9994a9efcb79163d85ff84cb92cc667e98b54934512a1f\
    65f4d5a173a69633d21748f4f181f6d4c512a65640bf599177e373bf42827b75da936e9116edc3437f932bad39d2051d\
    54fbf7054077ea68c2ace25c9b0229cfb8c63c120a4662c7c0710442c8214a10040087948d6db24c02c27eb106dc584c\
    dd92ef00758079cd8ed0c9ff443af9f4837f5bcc0361c6699bea06d19503748a8ca6943c2b69329401fb8266035c83fd\
    0fb311466fd9c372f8c857ca9d5699ead4fbb3305b816e6188eb07a0ecf8f4d5d94f88ed30b7594b5fe4ae6e4b6a6d8c\
    48fa1df573744366ac63f8a020c51f18346300030603fb042cd877b5679fa9e0e4a030446237317469c6546267347bef\
    fdea07cbd1fb7720ff92485a3a84406b90a56164aa0f49246be4aa4d684a1b85dc6c9ca8d46b0feaca868570d3336f8b\
    0593cc2ae54942911fa1ef6bba86ff05ba578257c489513d54d898ed957251d8f7b2f627e1f2b65fbb9ed46ddc1d1dad\
    2f8967380e29610000f74df4138bb664a39f241a5433e10c9bd566b068da419578b5e07447fab75bcb1121";
const K7_LENTYPE3_PUB: &str = "c6c0e20442c82148110400a9e34d4bea35eb2c1dbc737600e2c3373892a7c4cd464fd613916c84dd5cad7bae47f1653d\
    b463c15fb38cb9a5f978667ce0b1541af68fd24eda9df9977039023b73fd1a02feae8fdd4b63ecfef0a140f66a1dac44\
    c6928352de3b07b0c675126b0bc0d19c8478c1b4b2400ea66b74b5c85a82e692614acfa6a9ff5e1313a38b00a09edb61\
    925471218c37e76657f4ebfda21b7fa7f103ff55e2e0f4beb645d5bf2230e5a65f6bad5b4fa5ef0e4a0f234023dc6139\
    626087265280fc8844afc40938b57efb9544bf897a290d7f0c5a09ce835daf500626e9204dce97fd67104737d0b5489c\
    3c74267b9a005a81dae9b55064ec2f6c987798e5c3abf4e4fca176da0a3c99a338e2f327e1bbeff9205bb25be260b88b\
    8d927503ff495c5d5eb0dabe5fd1de849caa1c753b2c411ffdc93acc2ee8d9eaccdff2d9959b8b34f0f672fde82f736f\
    b1a12071349997ef1798ef292e6ce32886fe862ef4a95c77b7622132a6dbe01fdd318d4293ada03c129af35fd6c22eef\
    22c10140b12dc81986988eb8629ce844d15e69a17d258fac00aa913aa7ba74b8c9eef79b40bb0442c8214a1004008794\
    8d6db24c02c27eb106dc584cdd92ef00758079cd8ed0c9ff443af9f4837f5bcc0361c6699bea06d19503748a8ca6943c\
    2b69329401fb8266035c83fd0fb311466fd9c372f8c857ca9d5699ead4fbb3305b816e6188eb07a0ecf8f4d5d94f88ed\
    30b7594b5fe4ae6e4b6a6d8c48fa1df573744366ac63f8a020c51f18346300030603fb042cd877b5679fa9e0e4a03044\
    6237317469c6546267347beffdea07cbd1fb7720ff92485a3a84406b90a56164aa0f49246be4aa4d684a1b85dc6c9ca8\
    d46b0feaca868570d3336f8b0593cc2ae54942911fa1ef6bba86ff05ba578257c489513d54d898ed957251d8f7b2f627\
    e1f2b65fbb9ed46ddc1d1dad2f8967380e2961";
const K7_LENTYPE3_SEC: &str = "c5c0fb0442c82148110400a9e34d4bea35eb2c1dbc737600e2c3373892a7c4cd464fd613916c84dd5cad7bae47f1653d\
    b463c15fb38cb9a5f978667ce0b1541af68fd24eda9df9977039023b73fd1a02feae8fdd4b63ecfef0a140f66a1dac44\
    c6928352de3b07b0c675126b0bc0d19c8478c1b4b2400ea66b74b5c85a82e692614acfa6a9ff5e1313a38b00a09edb61\
    925471218c37e76657f4ebfda21b7fa7f103ff55e2e0f4beb645d5bf2230e5a65f6bad5b4fa5ef0e4a0f234023dc6139\
    626087265280fc8844afc40938b57efb9544bf897a290d7f0c5a09ce835daf500626e9204dce97fd67104737d0b5489c\
    3c74267b9a005a81dae9b55064ec2f6c987798e5c3abf4e4fca176da0a3c99a338e2f327e1bbeff9205bb25be260b88b\
    8d927503ff495c5d5eb0dabe5fd1de849caa1c753b2c411ffdc93acc2ee8d9eaccdff2d9959b8b34f0f672fde82f736f\
    b1a12071349997ef1798ef292e6ce32886fe862ef4a95c77b7622132a6dbe01fdd318d4293ada03c129af35fd6c22eef\
    22c10140b12dc81986988eb8629ce844d15e69a17d258fac00aa913aa7ba74b8c9eef79b4000009e3e22e2cb7eb3e0ba\
    58a4c80dc2f5232d126aaf250a9e9f0442c8214a10040087948d6db24c02c27eb106dc584cdd92ef00758079cd8ed0c9\
    ff443af9f4837f5bcc0361c6699bea06d19503748a8ca6943c2b69329401fb8266035c83fd0fb311466fd9c372f8c857\
    ca9d5699ead4fbb3305b816e6188eb07a0ecf8f4d5d94f88ed30b7594b5fe4ae6e4b6a6d8c48fa1df573744366ac63f8\
    a020c51f18346300030603fb042cd877b5679fa9e0e4a030446237317469c6546267347beffdea07cbd1fb7720ff9248\
    5a3a84406b90a56164aa0f49246be4aa4d684a1b85dc6c9ca8d46b0feaca868570d3336f8b0593cc2ae54942911fa1ef\
    6bba86ff05ba578257c489513d54d898ed957251d8f7b2f627e1f2b65fbb9ed46ddc1d1dad2f8967380e29610000f74d\
    f4138bb664a39f241a5433e10c9bd566b068da419578b5e07447fab75bcb1121";
const K8_TWO_MAIN_PUB: &str = "c6c0e20442c82148110400a9e34d4bea35eb2c1dbc737600e2c3373892a7c4cd464fd613916c84dd5cad7bae47f1653d\
    b463c15fb38cb9a5f978667ce0b1541af68fd24eda9df9977039023b73fd1a02feae8fdd4b63ecfef0a140f66a1dac44\
    c6928352de3b07b0c675126b0bc0d19c8478c1b4b2400ea66b74b5c85a82e692614acfa6a9ff5e1313a38b00a09edb61\
    925471218c37e76657f4ebfda21b7fa7f103ff55e2e0f4beb645d5bf2230e5a65f6bad5b4fa5ef0e4a0f234023dc6139\
    626087265280fc8844afc40938b57efb9544bf897a290d7f0c5a09ce835daf500626e9204dce97fd67104737d0b5489c\
    3c74267b9a005a81dae9b55064ec2f6c987798e5c3abf4e4fca176da0a3c99a338e2f327e1bbeff9205bb25be260b88b\
    8d927503ff495c5d5eb0dabe5fd1de849caa1c753b2c411ffdc93acc2ee8d9eaccdff2d9959b8b34f0f672fde82f736f\
    b1a12071349997ef1798ef292e6ce32886fe862ef4a95c77b7622132a6dbe01fdd318d4293ada03c129af35fd6c22eef\
    22c10140b12dc81986988eb8629ce844d15e69a17d258fac00aa913aa7ba74b8c9eef79b40c6c0e20442c82148110400\
    a9e34d4bea35eb2c1dbc737600e2c3373892a7c4cd464fd613916c84dd5cad7bae47f1653db463c15fb38cb9a5f97866\
    7ce0b1541af68fd24eda9df9977039023b73fd1a02feae8fdd4b63ecfef0a140f66a1dac44c6928352de3b07b0c67512\
    6b0bc0d19c8478c1b4b2400ea66b74b5c85a82e692614acfa6a9ff5e1313a38b00a09edb61925471218c37e76657f4eb\
    fda21b7fa7f103ff55e2e0f4beb645d5bf2230e5a65f6bad5b4fa5ef0e4a0f234023dc6139626087265280fc8844afc4\
    0938b57efb9544bf897a290d7f0c5a09ce835daf500626e9204dce97fd67104737d0b5489c3c74267b9a005a81dae9b5\
    5064ec2f6c987798e5c3abf4e4fca176da0a3c99a338e2f327e1bbeff9205bb25be260b88b8d927503ff495c5d5eb0da\
    be5fd1de849caa1c753b2c411ffdc93acc2ee8d9eaccdff2d9959b8b34f0f672fde82f736fb1a12071349997ef1798ef\
    292e6ce32886fe862ef4a95c77b7622132a6dbe01fdd318d4293ada03c129af35fd6c22eef22c10140b12dc81986988e\
    b8629ce844d15e69a17d258fac00aa913aa7ba74b8c9eef79b40cec04d0442c8214a10040087948d6db24c02c27eb106\
    dc584cdd92ef00758079cd8ed0c9ff443af9f4837f5bcc0361c6699bea06d19503748a8ca6943c2b69329401fb826603\
    5c83fd0fb311466fd9c372f8c857ca9d5699ead4fbb3305b816e6188eb07a0ecf8f4d5d94f88ed30b7594b5fe4ae6e4b\
    6a6d8c48fa1df573744366ac63f8a020c51f18346300030603fb042cd877b5679fa9e0e4a030446237317469c6546267\
    347beffdea07cbd1fb7720ff92485a3a84406b90a56164aa0f49246be4aa4d684a1b85dc6c9ca8d46b0feaca868570d3\
    336f8b0593cc2ae54942911fa1ef6bba86ff05ba578257c489513d54d898ed957251d8f7b2f627e1f2b65fbb9ed46ddc\
    1d1dad2f8967380e2961";

fn unhex(s: &str) -> Vec<u8> {
    let b = s.as_bytes();
    (0..b.len() / 2)
        .map(|i| u8::from_str_radix(core::str::from_utf8(&b[2 * i..2 * i + 2]).unwrap(), 16).unwrap())
        .collect()
}

fn ctl_pub() -> Vec<u8> {
    unhex(CTL_PUB)
}
fn ctl_sec() -> Vec<u8> {
    unhex(CTL_SEC)
}
fn msg1() -> Vec<u8> {
    unhex(MSG1)
}

fn cipher_msg(op: &str, r: Result<Vec<u8>, cipher::CipherError>) -> String {
    match r {
        Ok(_) => "OK".to_string(),
        Err(e) => cipher_err(op, e).message,
    }
}

// px.c:273 parse_cipher_name + pgcrypto.c:511 find_provider: option errors
// are distinct from "No such cipher algorithm"; empty option segments skip.
#[test]
fn cipher_spec_option_errors_are_c_exact() {
    let d = b"0123456789abcdef";
    let k = b"key";
    assert_eq!(
        cipher_msg("encrypt", cipher::encrypt("aes/badopt:1", k, &[], d)),
        "Cannot use \"aes/badopt:1\": Unknown option"
    );
    assert_eq!(
        cipher_msg("encrypt", cipher::encrypt("nope/x:1", k, &[], d)),
        "Cannot use \"nope/x:1\": Unknown option"
    );
    assert_eq!(
        cipher_msg("encrypt", cipher::encrypt("aes/pad", k, &[], d)),
        "Cannot use \"aes/pad\": Badly formatted type"
    );
    assert_eq!(
        cipher_msg("encrypt", cipher::encrypt("aes/pad:bogus", k, &[], d)),
        "Cannot use \"aes/pad:bogus\": No such cipher algorithm"
    );
    assert_eq!(cipher_msg("encrypt", cipher::encrypt("aes//pad:pkcs/", k, &[], d)), "OK");
    assert_eq!(
        cipher::encrypt("aes//pad:pkcs/", k, &[], d).ok(),
        cipher::encrypt("aes", k, &[], d).ok()
    );
}

// pgcrypto.c:288 pg_encrypt (and pg_decrypt / _iv): cipher failures are
// ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION; only find_provider's
// "Cannot use" is ERRCODE_INVALID_PARAMETER_VALUE (pgcrypto.c:513).
#[test]
fn cipher_failures_are_39000_and_provider_lookup_22023() {
    assert_eq!(
        cipher_err("decrypt", cipher::CipherError::DecryptFailed).sqlstate,
        ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION
    );
    assert_eq!(
        cipher_err("encrypt", cipher::CipherError::EncryptFailed).sqlstate,
        ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION
    );
    let e = cipher::encrypt("nope-cbc", b"k", &[], b"x").err().unwrap();
    assert_eq!(cipher_err("encrypt", e).sqlstate, ERRCODE_INVALID_PARAMETER_VALUE);
}

// px.c:204 combo_init: `if (klen > ks) klen = ks` — oversize keys are cut to
// the cipher's max key size (openssl.c cipher table), never refused.
#[test]
fn oversize_keys_clamp_to_cipher_key_size() {
    let d = b"0123456789abcdef";
    let enc = |spec: &str, key: &[u8]| cipher::encrypt(spec, key, &[], d).ok().expect(spec);
    assert_eq!(enc("aes", &[b'a'; 40]), enc("aes", &[b'a'; 32]));
    assert_eq!(enc("aes", &[b'a'; 33]), enc("aes", &[b'a'; 32]));
    assert_eq!(enc("bf", &[b'b'; 60]), enc("bf", &[b'b'; 56]));
    assert_eq!(enc("cast5", &[b'c'; 20]), enc("cast5", &[b'c'; 16]));
    assert_eq!(enc("3des", &[b'd'; 30]), enc("3des", &[b'd'; 24]));
    let ct = enc("aes", &[b'a'; 40]);
    assert_eq!(cipher::decrypt("aes", &[b'a'; 40], &[], &ct).ok().unwrap(), d);
}

// pgcrypto.c:504 find_provider: downcase_truncate_identifier before lookup
// AND before the "Cannot use" message (NAMEDATALEN-1 = 63 bytes).
#[test]
fn provider_names_are_downcased_and_truncated() {
    let x70 = "x".repeat(70);
    assert_eq!(
        hashing::digest(&x70, b"abc").unwrap_err(),
        format!("Cannot use \"{}\": No such hash algorithm", "x".repeat(63))
    );
    assert_eq!(
        hashing::digest("SHA9", b"abc").unwrap_err(),
        "Cannot use \"sha9\": No such hash algorithm"
    );
    assert_eq!(hashing::digest("SHA256", b"abc").unwrap(), hashing::digest("sha256", b"abc").unwrap());
    assert_eq!(
        hashing::hmac(&"Y".repeat(64), b"k", b"abc").unwrap_err(),
        format!("Cannot use \"{}\": No such hash algorithm", "y".repeat(63))
    );
    assert_eq!(
        cipher_msg("encrypt", cipher::encrypt(&"z".repeat(64), b"k", &[], b"x")),
        format!("Cannot use \"{}\": No such cipher algorithm", "z".repeat(63))
    );
}

// pgp.c:364 pgp_set_symkey: an empty key is PXE_ARGUMENT_ERROR on both the
// encrypt and the decrypt wrapper (pgp-pgsql.c:440 / :505).
#[test]
fn empty_symmetric_key_is_illegal_argument() {
    assert_eq!(
        pgp::sym_encrypt(b"Secret.", b"", None, true).unwrap_err(),
        "Illegal argument to function"
    );
    let ct = pgp::sym_encrypt(b"Secret.", b"k", None, true).unwrap();
    assert_eq!(
        pgp::sym_decrypt(&ct, b"", None, true).err().unwrap().message,
        "Illegal argument to function"
    );
}

// pgp-decrypt.c:647 parse_symenc_sesskey: ctx->s2k_count is decoded from
// s2k.iter for EVERY S2K mode (iter is 0 => 1024 for simple/salted).
#[test]
fn s2k_count_is_decoded_for_every_s2k_mode() {
    for mode in [b"s2k-mode=0".as_slice(), b"s2k-mode=1"] {
        let ct = pgp::sym_encrypt(b"Secret.", b"foobar", Some(mode), true).unwrap();
        let out = pgp::sym_decrypt(&ct, b"foobar", Some(b"expect-s2k-count=1024"), true).unwrap();
        assert_eq!(out.notices, Vec::<String>::new(), "{}", String::from_utf8_lossy(mode));
        let out = pgp::sym_decrypt(&ct, b"foobar", Some(b"expect-s2k-count=65536"), true).unwrap();
        assert_eq!(
            out.notices,
            vec!["pgp_decrypt: unexpected s2k_count: expected 65536 got 1024".to_string()]
        );
    }
}

// pgp-decrypt.c:681/612: the encrypted session key must be exactly
// 1 + cipher key size bytes; trailing bytes are PXE_PGP_CORRUPT_DATA.
#[test]
fn encrypted_session_key_length_must_match_cipher() {
    let ct = pgp::sym_encrypt(b"Secret.", b"key", Some(b"sess-key=1"), true).unwrap();
    assert_eq!(ct[0], 0xC3);
    let len = ct[1] as usize;
    let mut bad = vec![0xC3, (len + 3) as u8];
    bad.extend_from_slice(&ct[2..2 + len]);
    bad.extend_from_slice(&[1, 2, 3]);
    bad.extend_from_slice(&ct[2 + len..]);
    assert_eq!(pgp::sym_decrypt(&ct, b"key", None, true).unwrap().plaintext, b"Secret.");
    assert_eq!(
        pgp::sym_decrypt(&bad, b"key", None, true).err().unwrap().message,
        "Wrong key or corrupt data"
    );
}

// pgp-pubkey.c:84 calc_key_id hashes the MPI header bit count as read from
// the packet (pgp_mpi_hash), not a recomputed one: leading zero bits change
// the key id.
#[test]
fn key_id_hashes_mpi_header_bits() {
    assert_eq!(pgp::key_id(&ctl_pub()), Ok("D936CF64BB73F466".to_string()));
    assert_eq!(pgp::key_id(&unhex(K1_LEADZERO_PUB)), Ok("CE9583A4E37FD157".to_string()));
}

// pgp-info.c:156 pgp_get_keyid: a subkey that _pgp_read_public_key rejects
// aborts with that error; pgp-info.c:151/205: several keys is
// PXE_PGP_MULTIPLE_KEYS.
#[test]
fn key_id_propagates_subkey_errors() {
    assert_eq!(
        pgp::key_id(&unhex(K2_UNKNOWN_ALGO_PUB)).unwrap_err(),
        "Unknown public-key encryption algorithm"
    );
    assert_eq!(
        pgp::key_id(&unhex(K3_V3_TRUNC_PUB)).unwrap_err(),
        "Only V4 key packets are supported"
    );
    assert_eq!(pgp::key_id(&unhex(K7_LENTYPE3_PUB)).unwrap_err(), "Wrong key or corrupt data");
    assert_eq!(pgp::key_id(&unhex(K7_LENTYPE3_SEC)).unwrap_err(), "Wrong key or corrupt data");
    assert_eq!(
        pgp::key_id(&unhex(K8_TWO_MAIN_PUB)).unwrap_err(),
        "Several keys given - pgcrypto does not handle keyring"
    );
}

// pgp-pubkey.c:170 _pgp_read_public_key: the version byte is checked before
// anything else is read, so a 2-byte V3 subkey is PXE_PGP_NOT_V4_KEYPKT.
#[test]
fn v4_check_precedes_packet_length_check() {
    assert_eq!(
        pgp::pub_encrypt(b"msg", &unhex(K3_V3_TRUNC_PUB), None, true).unwrap_err(),
        "Only V4 key packets are supported"
    );
    assert_eq!(
        pgp::pub_encrypt(b"msg", &unhex(K2_UNKNOWN_ALGO_PUB), None, true).unwrap_err(),
        "Unknown public-key encryption algorithm"
    );
}

// pgp-pubkey.c:483 internal_read_key / pgp-info.c:133: allow_ctx = 0 — an
// old-format indeterminate-length packet header is PXE_PGP_CORRUPT_DATA in
// key material.
#[test]
fn indeterminate_length_packets_are_rejected_in_keys() {
    assert!(pgp::pub_encrypt(b"msg", &ctl_pub(), None, true).is_ok());
    assert_eq!(
        pgp::pub_encrypt(b"msg", &unhex(K7_LENTYPE3_PUB), None, true).unwrap_err(),
        "Wrong key or corrupt data"
    );
    assert_eq!(
        pgp::pub_decrypt(&msg1(), &unhex(K7_LENTYPE3_SEC), None, None, true).err().unwrap().message,
        "Wrong key or corrupt data"
    );
}

// pgp-pubkey.c:449 process_secret_key: pgp_expect_packet_end after the
// checksum — trailing bytes in a secret subkey packet are corrupt data.
#[test]
fn secret_key_trailing_bytes_are_rejected() {
    assert_eq!(
        pgp::pub_decrypt(&msg1(), &ctl_sec(), None, None, true).unwrap().plaintext,
        b"Secret msg"
    );
    assert_eq!(
        pgp::pub_decrypt(&msg1(), &unhex(K4_TRAILING_SEC), None, None, true).err().unwrap().message,
        "Wrong key or corrupt data"
    );
}

// pgp-pubkey.c:331 check_key_cksum (and :290 check_key_sha1): a mismatch is
// PXE_PGP_KEYPKT_CORRUPT ("Corrupt key packet"), not the generic corrupt-data.
#[test]
fn secret_key_checksum_mismatch_is_corrupt_key_packet() {
    assert_eq!(
        pgp::pub_decrypt(&msg1(), &unhex(K5_BADCKSUM_SEC), None, None, true).err().unwrap().message,
        "Corrupt key packet"
    );
}

// pgp-pubkey.c:412 process_secret_key reads the FULL secret material of
// signing subkeys (RSA sign: d,p,q,u; DSA: x) and validates it; the key is
// then skipped (can_encrypt = 0) and the encryption subkey found.
#[test]
fn signing_subkeys_are_parsed_in_full_then_skipped() {
    for (name, key) in [("dsa", K6_DSA_SIGNSUB_SEC), ("rsa-sign", K6B_RSA_SIGNSUB_SEC)] {
        let out = pgp::pub_decrypt(&msg1(), &unhex(key), None, None, true)
            .unwrap_or_else(|e| panic!("{name}: {}", e.message));
        assert_eq!(out.plaintext, b"Secret msg", "{name}");
    }
}

struct ForceRandomFailure;
impl ForceRandomFailure {
    fn arm() -> ForceRandomFailure {
        consts::FORCE_RANDOM_FAILURE.with(|f| f.set(true));
        ForceRandomFailure
    }
}
impl Drop for ForceRandomFailure {
    fn drop(&mut self) {
        consts::FORCE_RANDOM_FAILURE.with(|f| f.set(false));
    }
}

// px.c:96 px_THROW_ERROR(PXE_NO_RANDOM): every pg_strong_random failure in
// the pgp paths (pgp-s2k.c:237, pgp-encrypt.c:487/586, pgp-pubenc.c:55) and
// pgcrypto.c:471 pg_random_bytes surfaces as "could not generate a random
// number" with ERRCODE_INTERNAL_ERROR.
#[test]
fn rng_failures_report_pxe_no_random() {
    let _f = ForceRandomFailure::arm();
    let want = "could not generate a random number";
    assert_eq!(pgp::sym_encrypt(b"x", b"k", Some(b"s2k-mode=1"), true).unwrap_err(), want);
    assert_eq!(pgp::sym_encrypt(b"x", b"k", Some(b"s2k-mode=3"), true).unwrap_err(), want);
    assert_eq!(pgp::sym_encrypt(b"x", b"k", Some(b"s2k-mode=0, sess-key=1"), true).unwrap_err(), want);
    assert_eq!(pgp::sym_encrypt(b"x", b"k", Some(b"s2k-mode=0"), true).unwrap_err(), want);
    assert_eq!(pgp::pub_encrypt(b"x", &ctl_pub(), None, true).unwrap_err(), want);
    let e = random_bytes(16).unwrap_err();
    assert_eq!(e.message, want);
    assert_eq!(e.sqlstate, ERRCODE_INTERNAL_ERROR);
    assert_eq!(px_msg(want).sqlstate, ERRCODE_INTERNAL_ERROR);
}

// pgp-s2k.c:247 pgp_s2k_fill: an unknown mode is PXE_PGP_BAD_S2K_MODE.
#[test]
fn s2k_fill_bad_mode_message() {
    assert_eq!(crate::pgp::s2k::S2k::fill(7, 2, -1).err(), Some("Bad S2K mode"));
}

struct ForceFipsMode;
impl ForceFipsMode {
    fn arm() -> ForceFipsMode {
        FORCE_FIPS_MODE.with(|f| f.set(true));
        ForceFipsMode
    }
}
impl Drop for ForceFipsMode {
    fn drop(&mut self) {
        FORCE_FIPS_MODE.with(|f| f.set(false));
    }
}

// openssl.c:888 CheckBuiltinCryptoMode over pgcrypto.builtin_crypto_enabled:
// "off" always errors, "fips" errors iff the linked OpenSSL is in FIPS mode
// (openssl.c:847 CheckFIPSMode — the vendored build never is), "on" passes.
// Both ereports carry C's default errcode (XX000).
#[test]
fn builtin_crypto_mode_off_and_fips_arms() {
    assert!(check_builtin_crypto_mode(Some("on")).is_ok());
    assert!(check_builtin_crypto_mode(None).is_ok());
    let off = check_builtin_crypto_mode(Some("off")).err().unwrap();
    assert_eq!(off.message, "use of built-in crypto functions is disabled");
    assert_eq!(off.sqlstate, ERRCODE_INTERNAL_ERROR);
    assert!(!check_fips_mode(), "vendored OpenSSL has no FIPS provider");
    assert!(check_builtin_crypto_mode(Some("fips")).is_ok());
    let _f = ForceFipsMode::arm();
    assert!(check_fips_mode());
    let e = check_builtin_crypto_mode(Some("fips")).err().unwrap();
    assert_eq!(
        e.message,
        "use of non-FIPS validated crypto not allowed when OpenSSL is in FIPS mode"
    );
    assert_eq!(e.sqlstate, ERRCODE_INTERNAL_ERROR);
    assert!(check_builtin_crypto_mode(Some("on")).is_ok());
}
