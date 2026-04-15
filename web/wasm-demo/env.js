const SQRT_PI = Math.sqrt(Math.PI);
const LANCZOS_G = 7;
const LANCZOS_COEFFS = [
  0.9999999999998099,
  676.5203681218851,
  -1259.1392167224028,
  771.3234287776531,
  -176.6150291621406,
  12.507343278686905,
  -0.13857109526572012,
  9.984369578019572e-6,
  1.5056327351493116e-7,
];

function erfApprox(x) {
  const sign = x < 0 ? -1 : 1;
  const ax = Math.abs(x);
  const t = 1 / (1 + 0.3275911 * ax);
  const y =
    1 -
    (((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) *
      t +
      0.254829592) *
      t *
      Math.exp(-ax * ax));
  return sign * y;
}

function logGamma(z) {
  if (Number.isNaN(z)) {
    return NaN;
  }
  if (!Number.isFinite(z)) {
    return Infinity;
  }
  if (z < 0.5) {
    return Math.log(Math.PI) - Math.log(Math.sin(Math.PI * z)) - logGamma(1 - z);
  }
  let x = LANCZOS_COEFFS[0];
  const zm1 = z - 1;
  for (let i = 1; i < LANCZOS_COEFFS.length; i++) {
    x += LANCZOS_COEFFS[i] / (zm1 + i);
  }
  const t = zm1 + LANCZOS_G + 0.5;
  return (
    0.5 * Math.log(2 * Math.PI) +
    (zm1 + 0.5) * Math.log(t) -
    t +
    Math.log(x)
  );
}

function gamma(z) {
  if (Number.isNaN(z)) {
    return NaN;
  }
  if (!Number.isFinite(z)) {
    return z > 0 ? Infinity : NaN;
  }
  if (z < 0.5) {
    return Math.PI / (Math.sin(Math.PI * z) * gamma(1 - z));
  }
  return Math.exp(logGamma(z));
}

export function erf(x) {
  return erfApprox(x);
}

export function erfc(x) {
  return 1 - erfApprox(x);
}

export function tgamma(x) {
  return gamma(x);
}

export function lgamma(x) {
  return logGamma(x);
}

export default { erf, erfc, tgamma, lgamma };
