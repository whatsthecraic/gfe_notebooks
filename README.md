# Notebooks for the GFE experiments

This repository contains the notebooks used to analyse and to generate the plots featured in the [GFE Driver](https://github.com/cwida/gfe_driver). Some notebooks are in the Jupyter format and some in Mathematica v12.1 (sorry!).

After downloading repository, fetch the database with the results from [Zenodo](https://zenodo.org/record/4534418) and place it into data/data21.sqlite3. It is a 600 MB database and it was a bit too much to store it in this repository.

The content of this repository: 

* automerge.pl: a script to load the results of new executions of the [GFE Driver](https://github.com/cwida/gfe_driver) into the database data/data21.sqlite3.
* bm.nb: the notebook (Mathematica) to generate the plot of Figure 9 in the paper.
* example.ipynb: a sample notebook to analyse the results for the experiments with insertions in Jupyter.
* gapbs_speedup.ipynb: to generate the plot of Figure 8 in the paper. This is the difference in completion time of the native algorithms shipped by Stinger, LLAMA and GraphOne versus those provided by the GAP BS. 
* graphalytics_data.ipynb: sample notebook to visualize the results of Graphalytics.
* graphalytics_gen_table.ipynb: the notebook used to generate Table 3 in the paper, that is, the results from Graphalytics.
* insertions.nb: the notebook (Mathematica) to generate the plot of Figure 6 in the paper.
* pip_freeze.txt: dependendencies for the Python environment and Jupyter.
* updates.nb: the notebook (Mathematica) to generate the plot of Figure 7 in the paper.
* views.sql: list of supplementary SQL views (already loaded in data/data21.sqlite3) to query the results of the experiments. 

