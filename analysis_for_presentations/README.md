# Data Citation Corpus Analysis for Presentations

## ACRL 2025 Poster

### Data citations for University of Colorado, Boulder and Northwestern University

We are bringing in citations from Europe PMC.

```mermaid
erDiagram
    EUROPEPMC {
        string target "cited data — either DOI or accession number"
        string PMCID
        string EXTID
        string SOURCE
        string cited_repository "From EuropePMC, or from DOI metadata (API)"
        bool cited_is_doi "If false, the cited data is an accession number. If true, it is a DOI"
        string citing_doi
        string[] cited_subjects "From Corpus or DataCite/Crossref API or manual mapping through repo"
        string[] cited_funders "Can come from Corpus or from DataCite/Crossref API"
        string cited_title "From DataCite/Crossref API"
        string citing_openalex_id
        bool citing_is_oa
        string citing_openalex_type
        string[] citing_institutions_ror
        string citing_primary_topic "Using OpenAlex Topics"
        string citing_topic_subfield "Using OpenAlex Topics"
        string citing_topic_field "Using OpenAlex Topics"
        string citing_topic_domain "Using OpenAlex Topics"
        string[] citing_openalex_funders
        string[] citing_openalex_datasets
        bool affil_ucboulder "True if citing publication has the affiliation (from OpenAlex)"
        bool affil_northwestern "True if citing publication has the affiliation (from OpenAlex)"
        string target_upper
        string corpus_id "If null, this citation is not in the Corpus"
        bool corpus_has_affils
        bool doi_api_has_affils
        int publication_year "for citing publication"
    }
```
