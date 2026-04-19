# domains/

Terraform for `pgrust.com` — Route 53 zone, ACM cert, CloudFront distribution fronting the private S3 bucket `pgrust`, www→apex redirect via a CloudFront Function.

## Architecture

```
https://www.pgrust.com → CloudFront Function (301) → https://pgrust.com
https://pgrust.com     → Route 53 ALIAS → CloudFront → (OAC) → s3://pgrust
```

## First-time setup

The Route 53 hosted zone was auto-created by Route 53 Registrar when the domain was registered. Import it into terraform state before the first apply:

```bash
cd pgrust/domains
AWS_PROFILE=mfa terraform init
AWS_PROFILE=mfa terraform import aws_route53_zone.pgrust_com Z0795405IN00H8WFZEZA
AWS_PROFILE=mfa terraform plan
AWS_PROFILE=mfa terraform apply
```

The apply blocks ~2–5 min on ACM DNS validation, then ~5–15 min on CloudFront distribution deploy.

## Regular operation

After first apply, subsequent changes are just `terraform apply`. Registrar-level nameservers already point at this zone — no manual AWS Console step needed.

## Content deploy

Infrastructure and content are separate concerns. To push site changes:

```bash
cd ../web/wasm-demo
./deploy.sh
```

The deploy script reads `cloudfront_distribution_id` from `terraform output` and invalidates the cache automatically.

## Cost

Expected ~$0.50–2/month at demo traffic:
- Route 53 hosted zone: $0.50/mo
- CloudFront: free for first 1 TB/month, then ~$0.085/GB
- S3 storage: pennies for a <50 MB site
- ACM cert: free

If traffic spikes unexpectedly, CloudFront data transfer is the lever that can run up. Set an AWS Budget alert (~$20/mo threshold) on the account if not already done.

## Migrating to Cloudflare Pages / Vercel later

Swap CloudFront + ACM + S3 resources here for CNAME records pointing at the new host. No lock-in from the current design (no Lambda@Edge, no signed URLs, just a static-origin CDN).
