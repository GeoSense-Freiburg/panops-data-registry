find /mnt/ssds/dl1070/cit-sci-traits/data/processed/Shrub_Tree_Grass/1km/final/ -name "*.tif" | while read file; do
  filename=$(basename "$file")
    dvc import-url "$file" "data/trait_maps/lusk_et-al_2025/cwms/Shrub_Tree_Grass/1km/$filename"
done

