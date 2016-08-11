The code in this folder reproduces almost all the covariates which were present in the MIMIC-II study.
Many of these covariates were not used in the final model as the feature selection algorithm skipped them.

Steps to reproduce:

1. Run all the sql scripts in `../sql` in order to have the necessary views ready
2. Run all the sql scripts in this folder, except `data-full.sql`
3. Run `data-full.sql` - the output is all covarites

The following covariates were present in the original study, but are not extracted here:

1. Blood transfusions
2. Fluid intake and outtake
3. IV fluid intake and outtake
4. Arterial blood gas measurements: PaO2, PaCO2, pH
5. Number of arterial/venous blood gas measurements
