# re_investing_analyses

## Purpose and Structure of Codebase
### Purpose
This codebase is intended to model dynamics for understanding good markets for investing in Real Estate,
 both by and within MSAs in the United States.

To date, there have been four key areas that were explored, following literature I've read, as well as pulling threads 
through the course of the review:
* *Economic Trends:* By MSA, 5y CAGR of GDP, wage growth, population growth, industry dynamics
* *Housing Market Trends:* By MSA, appreciation, days-on-market, and months-of-inventory trends (last 10y)
* *Housing Construction Trends:* By MSA, job growth vs. new construction permits requested
* *Price-to-Rent Trends:* By MSA, home vs. rent appreciation (last 10y)

When determining attractive market to invest in, these criteria are weighted differently (and some negatively - 
for example, "Peak" markets or high "Price-to-Rent" ratios). This weighting can be found in the config, and the 
different data sources tested for the analysis can be found in the cell below this one.

Cutoffs for classifications were also created following the reading:
* *Days-on-Market:* A 1m average of 60 DOM is one criteria for delineating between balanced and sellers markets.
* *Months of Inventory:* A 6m rolling average of 4 MOI is one criteria for delineating between balanced and sellers markets
* *Price-to-Rent Ratios:* A PtR <15 is a buyers market; a PtR >21 is a renters market. In between is "moderate."
* *Construction Ratios:* A "Jobs-to-Permit" ratio <1.5 has a risk of oversupply; a JtP ratio >2.5 has a risk of undersupply.
* *Economic Trends:* We tag the Top 50 markets for best performance along the trends outlined.

Ultimately, we are trying to find MSAs that have positive economic tailwinds (particularly with regard to STEM jobs a
nd an educated workforce), good local governance, and are expanding in the housing cycle (decreasing DOM, but some 
slack in inventory).

### Structure of Codebase
```
├── LICENSE
├── README.md          <- The top-level README for developers using this project.
|
├── docs               <- A default Sphinx project; see sphinx-doc.org for details
│
├── projects           <- Jupyter notebooks of analysis. Each analysis is its own folder.
│
├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
│                         generated with `pip freeze > requirements.txt`
│
├── src                <- Source code for use in this project.
│   ├── run.py         <- Allows src to be run as a Python module
│   ├── config.yaml    <- Config for editing inputs. Use Box.
│   │
│   ├── trends          <- Underlying phenomena codebase is trying to model.
│   │   └── economic_trends.py
│   │   └── housing_trends.py
│   │   └── pricing_trends.py
│   │   └── shortage_trends.py
│   │
│   ├── utils       <- Helper files for core logic of module
│   
└── tests           <- Unit tests
```

## How to Run
1. `git clone` the development branch 
2. Set up an environment and `pip install -r requirements.txt`
3. Get credentials for the government websites and Google Sheets API. Store in `env/` folder.
4. Ask me for crosswalk / mapping table. Redfin Data can be pulled from their Data Center site.

## Outstanding Steps
### Hardening Codebase
* Move any data calls into `data.py` file. Will help with testing and remove potential side effects.
* Add `MyPy` and `typing` for type checking. Loading CSVs are a bit of a mess, particularly if Excel is opened 
within an analysis.
* Add `pytest` unit tests.
* Set up CI with Circle CI.

### Expanding Utility of Code 
* Refine weighting of investing metrics in config. Current balance does not reflect investing priorities.
* Tie the timing of the data sources, understand the rate at which updates can be scheduled, and add scheduling 
/ orchestration to the output. I would like for this analysis to tell me when I should be giving attention to new
markets.
* Understand how to use views / analysis to develop an interactive application for exploring the attractiveness of
specific markets. Important to determine if this is additive / complementary to Redfin's Data Center.