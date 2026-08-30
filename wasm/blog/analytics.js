(() => {
  const key = "phc_pShx8jpdxg92kob8f5UncCwLiBNE4BXVdBhLqGhtxebj";
  const host = "https://us.i.posthog.com";
  const posthog = window.posthog = window.posthog || [];
  if (posthog.__SV) return;

  const stub = (name) => {
    posthog[name] = (...args) => posthog.push([name, ...args]);
  };
  ["capture", "identify", "reset", "set_config"].forEach(stub);
  posthog._i = [[key, { api_host: host, person_profiles: "identified_only" }]];
  posthog.__SV = 1;

  const script = document.createElement("script");
  script.async = true;
  script.src = "https://us-assets.i.posthog.com/static/array.js";
  document.head.appendChild(script);
})();

