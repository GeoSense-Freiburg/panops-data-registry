#!/bin/bash
# Copy the generated figures and tables into the coauthor draft under the names used in draft_coauthors.tex
# (2026-10-23; replaces the scratchpad helper used before). Run from anywhere: bash scripts/paper_v41/copy_to_coauthor_draft.sh
cd /mnt/gsdata/projects/panops/panops-data-registry/data/flux
D=manuscript_coauthor_draft; P=plots/paper_v5_nothresh; N=$P/nature; A=$N/appendix; T=manuscript/tables
c() { if [ -f "$1" ]; then cp "$1" "$D/figures/$2"; else echo "MISSING $1"; fi; }
# main text
c $N/AG_24m/fig1_sites_efps.png fig1_site_map.png
c $N/AG_24m/fig_skill_bars.png fig_skill_bars_AG.png
c $N/AG_24m/fig5_residual_by_mortality_class.png fig2_residual_by_mortality_AG_24m.png
c $N/AG_24m/fig2_net_mortality_shap_per_site_piyg_names.png fig3_signed_relmort_24m_AG_piyg_names.png
c $N/AG_24m/fig3_high_vs_lowmid.png fig4_relmort_high_vs_lowmid_AG.png
c $N/AG_24m/fig4_mortality_vs_direction.png fig5_relmort_direction_24m_AG.png
# appendix
c $A/site_mae_AG.png A_fig_MAE_AG.png
c $A/site_mae_AG_high.png A_fig_MAE_high_AG.png
c $A/site_drmse_AG.png A_fig_site_dRMSE_AG.png
c $A/siteyear_dabs_AG.png A_fig_siteyear_dabs_AG.png
c $N/AG_12m/fig2_net_mortality_shap_per_site_piyg_names.png A_fig_signed_12m_AG_sites.png
c $A/direction_rel_deadwood_AG.png A_fig_direction_rel_deadwood_AG.png
c $A/igbp_AG_24m.png A_fig_igbp_24m_AG.png
c $A/igbp_AG_12m.png A_fig_igbp_12m_AG.png
c $A/mortality_block_AG.png A_fig_mortality_block_AG.png
c $N/AG_12m/fig5_residual_by_mortality_class.png A_fig_residual_by_mortality_AG_12m.png
c $A/dose_robustness_AG.png A_fig_dose_robustness_AG.png
c $A/dose_siteyear_AG.png A_fig_dose_siteyear_AG.png
c $A/residual_efpclim_AG_24m.png fig6_residual_by_mortality_AG_efpclim_24m.png
c $A/shap_composition_AG_24m.png A_fig_shap_composition_AG_24m.png
c $A/shap_composition_AG_12m.png A_fig_shap_composition_AG_12m.png
# tables: label and float placement as used in the draft
cpt() { sed -e "s/\\\\label{[^}]*}/\\\\label{$3}/" -e "s/\\\\begin{table}\\[[a-z]*\\]/\\\\begin{table}[$4]/" $T/$1 > $D/tables/$2.tex; }
cpt table_bootstrap_dMAE_v5_nothresh_AG_siteyear.tex table1_bootstrap_siteyear_dMAE_AG tab:main htbp
cpt table_fig2_dMAE_total_mort_v5_nothresh_permlog_AG.tex tableA_site_logratio_dMAE_AG tab:a_site htbp
cpt table_bootstrap_dMAE_v5_nothresh_AG.tex tableA_bootstrap_full_dMAE_AG tab:a_boot_full htbp
cpt table_bootstrap_dRMSE_v5_nothresh_AG.tex tableA_bootstrap_full_dRMSE_AG tab:a_boot_rmse htbp
sed -e 's/\\label{[^}]*}/\\label{tab:efpclim}/' $T/table_bootstrap_dMAE_v5_nothresh_AGAGx_siteyear_efp_v5_nothresh_efpclim.tex > $D/tables/table_efpclim_bootstrap_dMAE.tex
# every figure and table the draft uses must exist
for f in $(grep -o 'figures/[^}]*' $D/draft_coauthors.tex); do [ -f "$D/$f" ] || echo "draft uses missing $f"; done
for f in $(grep -o 'input{tables/[^}]*' $D/draft_coauthors.tex | sed 's/input{//'); do [ -f "$D/$f.tex" ] || echo "draft uses missing $f"; done
echo COPIED
