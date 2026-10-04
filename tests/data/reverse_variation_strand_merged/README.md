# Reverse-strand known variants

These are **synthetic capability tests** for [vepyr#156](https://github.com/biodatageeks/vepyr/issues/156).
The native transcript, regulatory and motif context is a genuine merged116
GRCh38 slice around chr1:1,000,000. The twelve known-variant rows are deliberately
injected; their names, strands, frequencies and clinical labels are synthetic.
Neither DT-6dc0ecf2c5 nor DT-a49e2146f6 becomes a qualified natural-cache port.

The six normalized inputs each test a distinct behavior:

| Input | Expected behavior |
| --- | --- |
| snv | Match reverse C/A against G/T, reject reverse C/T and reverse G/T, preserve forward/null/zero and use the original multiallelic AF key A |
| mnv | Match reverse CC/AA against GG/TT |
| deletion | Match reverse C/- against anchored GG/G |
| clinical_reverse | Flip A:pathogenic because the clinical reference C differs from the live reference G |
| clinical_forward | Retain T:likely_pathogenic because its clinical reference already is G |
| clinical_missing | Retain T:uncertain_significance when no clinical reference is present |

Clinical terms are isolated at different loci because multiple terms in a VEP
CSQ entry have nondeterministic Perl hash ordering. No golden body is sorted or
rewritten. Every golden is actual Docker VEP 116.2 output from the pinned image,
`--everything --merged`, the committed synthetic native cache and full GRCh38
reference. `provenance.json` records the commands, input SHA-256 and body MD5.
The same normalized input bytes go to both engines.

The Python tests convert the native cache themselves, assert the physical
nullable Int8 strand and unchanged allele/AF labels, then compare complete VCF
bodies through plain/indexed input, one/two workers, API, CLI and LazyFrame.
The Rust tests additionally cover primary attachment without a co-located sink,
the unshifted matching path, and legacy converted schemas lacking strand.
Old caches that discarded strand need reconversion to support negative rows;
missing strand still means forward, as in VEP.

To regenerate the native slice and inputs, run `prepare.py` with
`DATA_VEPYR_DIR` pointing at downloaded merged116 data. It requires Docker,
bcftools (`BCFTOOLS` may select its path), samtools, bgzip and tabix.
`slice_native.pl` preserves the selected biological objects using Storable.
The compressed FASTA retains real sequence at 993966–1016540, original chr1
coordinates and N padding elsewhere. The generator also reruns all six Docker VEP oracles and records fresh commands,
exit status and body MD5s. It writes the provenance receipt only after every
oracle succeeds; it never reuses old oracle metadata for changed native data.

CI uses the padded fixture FASTA. Qualification also runs all six cases manually
against the full GRCh38 FASTA and records the results outside the committed fixture.
If regeneration fails, treat the directory as incomplete and rerun the complete
generator successfully before committing its inputs, goldens and provenance.

A separate Python regression constructs an UNKNOWN physical cache row to preserve
existing coordinate-only annotations for raw joined ALTs. This compatibility test
does not claim full VEP parity for unsplit multi-allelic annotation.
