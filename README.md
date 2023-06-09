## check-apphash

Pull a CometBFT log stream from GCP and record the reported app hashes.

This is to detect mismatches across a validator fleet early.

## Usage:

1. You must have set the `GOOGLE_APPLICATION_CREDENTIALS` env var (see https://cloud.google.com/docs/authentication/application-default-credentials)

2. `go mod tidy`

3. `go run main.go YOUR_GCP_PROJECT_ID`

e.g.

`go run main.go penumbra-sl-testnet`

