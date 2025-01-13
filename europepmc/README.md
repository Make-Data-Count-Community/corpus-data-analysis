# EuropePMC Data Citations

+ Bulk downloads from EuropePMC are available here: <https://europepmc.org/downloads>
  + This includes data files of "accession numbers in Europe PMC articles": <https://europepmc.org/ftp/TextMinedTerms>
  + If you are using an ftp client you can use this address to download all of them: <ftp://ftp.ebi.ac.uk>
  + It says that these are updated weekly, though I would want to verify that.
  + There are 52 data files in this folder, organized by repository.

## Data model and entity relationships

```mermaid
erDiagram
    CORPUS["Corpus Citation"] {
        string id PK
        string publication_id "Currently only DOI allowed"
        string dataset_id "DOI or accession number"
        category repository "not necessarily matched to the EuropePMC repository"
        bool publication_is_doi
        bool dataset_is_doi
    }
    EUROPEPMC["EuropePMC Accession Number Citation"] {
        string accession_number PK
        string PMCID PK
        category repository_europepmc
    }
    DOI_PMC["EuropePMC to DOI"] {
        string PMCID PK, FK
        string DOI PK, FK
    }
    REPOSITORY["EuropePMC Repository"] {
        category repository_europepmc PK, FK
    }
    CORPUS one to zero or one DOI_PMC : matches
    EUROPEPMC one to zero or one DOI_PMC : "identified by"
    EUROPEPMC one to one REPOSITORY : "in"
```

Relationships are drawn using [crow's foot notation](https://en.wikipedia.org/wiki/Entity%E2%80%93relationship_model#Crow's_foot_notation).