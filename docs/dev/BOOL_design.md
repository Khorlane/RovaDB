## BOOL Design

Schema type:
- `BOOL`

Runtime value:
- internal value kind: `ValueKindBool`
- Go type: `bool`
- `NULL` remains a separate existing value kind

Storage encoding:
- dedicated BOOL tag
- `TRUE` encoded as BOOL tag + `1`
- `FALSE` encoded as BOOL tag + `0`
- BOOL must not reuse INT or TEXT encoding
- decoding must remain backward-compatible with existing stored rows

Enforcement rules:
- BOOL columns accept `TRUE`, `FALSE`, and `NULL`
- reject INT values such as `0` and `1`
- reject TEXT values such as `'true'` and `'false'`
- comparisons are BOOL-to-BOOL only; no cross-type equality with INT or TEXT
