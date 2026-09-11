# Temporal precision metadata

ADR-017's 2026-09-09 refinement adds this public schema field:

```go
TimePrecision *int32 `json:"timePrecision,omitempty"`
```

- Only `DateTime` and `Timestamp` may declare it, including nested `Fields` and
  `List.Element` schemas. The inclusive range is 0 through 9.
- `nil` means unknown, not zero. JSON omits unknown precision; an explicit JSON
  `null` also decodes to unknown. A pointer to zero emits `"timePrecision":0`.
- `DateTimeValue` remains a timezone-free civil value. `Timestamp` still contains
  `time.Time`, encoded as a UTC instant. Both retain nanosecond representation.
- A present, non-null value with precision `p` must have nanoseconds divisible by
  `10^(9-p)`. Validation rejects mismatches and never rounds. Missing and NULL
  keep their existing semantics; invalid schema metadata is rejected even when
  the batch contains no records.

`Value.Validate() error` and `NewValue(Kind, any) (Value, error)` do not receive a
schema and therefore check intrinsic value validity only. Their public signatures
are unchanged. `Batch.Validate() error` supplies each field to the same recursive
value validator and enforces temporal alignment as well as existing decimal and
nullability constraints. `Encode` and `Decode` validate the complete batch.

The existing, unreleased `planx-record/1` codec carries the schema directly, so
the new lowerCamel field is preserved without a second codec or wire version.
Schemas omitting it remain valid and unknown. Both sides of an external boundary
must use the matching revised record contract. Schema-dependent saved cursor
fingerprints must fail closed after a metadata change; this package neither
resets cursors nor changes runtime identity or acknowledgments.

`Batch.Clone` and `Append` own copied precision pointers at every schema depth.
Append compatibility compares precision values, distinguishing unknown from
known zero, not pointer addresses. `EstimatedBytes` adds a constant conservative
eight-byte accounting allowance per non-nil precision pointer, including nested
fields and elements; it remains a validated-batch logical/retained-size estimate,
not an exact heap measurement.

SQL metadata, mapping propagation/conversion policy, target compatibility and UI
display remain owned by their existing domains. Do not infer precision from a
sample's fractional digits or use Decimal `Precision`/`Scale` for temporal data.
