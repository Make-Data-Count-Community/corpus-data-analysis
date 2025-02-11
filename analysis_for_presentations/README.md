# Data Citation Corpus Analysis for Presentations

## ACRL 2025 Poster

### Data citations for University of Colorado, Boulder and Northwestern University

We are bringing in citations from Europe PMC.

```mermaid
erDiagram
    EUROPEPMC {
        string target
        string PMCID
        string EXTID
        string SOURCE
        string cited_repository "From EuropePMC, or from DOI metadata (API)"
        bool cited_is_doi "If false, the cited data is an accession number. If true, it is a DOI"
        string citing_doi
        string[] cited_subjects
        string[] cited_funders
        string cited_title
        string citing_openalex_id
        bool citing_is_oa
        string citing_openalex_type
        string[] citing_institutions_ror
        string citing_primary_topic
        string citing_topic_subfield
        string citing_topic_field
        string citing_topic_domain
        string[] citing_openalex_funders
        string[] citing_openalex_datasets
        bool affil_ucboulder
        bool affil_northwestern
        string target_upper
        string corpus_id
        bool corpus_has_affils
        bool doi_api_has_affils
        int publication_year "for citing publication"
    }
```
