## Unreleased

### Improvements

### Changes

### Fixed

### Security

## v0.6.0 (Mori)

First release as Mori, 0xCarbon's maintained hard fork of
hashicorp/memberlist.

### Changes

* Module path renamed to `github.com/0xCarbon/mori`, package `memberlist`
  renamed to `mori`. No API changes.
* Removed HashiCorp release/compliance tooling (`tag.sh`, copywrite,
  CODEOWNERS); default branch renamed to `main`.

### Fork point

Forked from hashicorp/memberlist `master` at `371698b` (post-v0.5.4),
which includes unreleased upstream fixes: keyring concurrent read/write
(hashicorp#342), nil pointer dereference (hashicorp#336), broadcast data race
(hashicorp#273), probe selection for very small clusters (hashicorp#350), and
remote state header limits (hashicorp#357).
