library("dplyr")
library("tidyr")
library("EcoFoG")
library("lubridate")
library("sqldf")

GenerateCSVAppurement <- function(file = "example.csv") {
    ### Lit le CSV (format point-virgule) ###
    dataSource <- read.csv2(file)

    ### read.csv2 traite Circonference comme un facteur, cette ligne la convertit en format numérique
    dataSource$Circonference <- as.numeric(dataSource$Circonference)

    ### Charge la variable dataGuyafor si elle n'est pas déjà chargée
    if(!exists("dataGuyafor")) dataGuyafor <- Guyafor2df()

    ### Crée une variable année de mesure ###
    dataSource <- dataSource %>%
      mutate(AnneeMesure = as.numeric(year(mdy(DateMesure))))

    ### Determine quel couple Plot/Carré doit être pioché dans la bdd ###
    plotsOfInventory <- paste(dataSource$Libelle, dataSource$NumCarre, sep = '|') %>%
      as.factor() %>%
      levels()

    ### Crée des couples uniques de plots/subplots
    couplePlotSubplot <- strsplit(plotsOfInventory, "|", fixed = TRUE)

    ### Initialisation des listes
    data <-
      years <-
      dataLastInventory <-
      yearOfLastInventory <-
      diffYear <-
      recruits <-
      abnormalRecruits <-
      output <-
      mergedData <-
      duplicateTrees <-
      list()

    ### Pour chaque couple Plot/Subplot
    for (i in 1:length(couplePlotSubplot)) {
      ### Extraire dans une liste les données de chaque couple à partir du fichier d'entrée
      data[[i]] <- dataSource %>%
        filter(Libelle == couplePlotSubplot[[i]][1] & NumCarre == couplePlotSubplot[[i]][2])

      ### Extraire les données de chaque couple depuis Guyafor
      dataLastInventory[[i]] <- dataGuyafor %>%
        filter(Plot == couplePlotSubplot[[i]][1] & SubPlot == couplePlotSubplot[[i]][2])

      ### Enregistre la dernière année d'inventaire du plot correspondant
      yearOfLastInventory[[i]] <- max(dataLastInventory[[i]]$CensusYear)
      ### Enregistre l'année la plus élevée du fichier source
      years[[i]] <- max(data[[i]]$AnneeMesure)

      ### Filtre les données de la dernière année d'inventaire pour comparaison
      dataLastInventory[[i]] <- dataLastInventory[[i]] %>%
        filter(CensusYear == yearOfLastInventory[[i]])

      ### Différence entre l'année de la dernière inventaire et l'année recensée
      diffYear[[i]] <- years[[i]] - yearOfLastInventory[[i]]

      ### Variable temporaires
      tempData <- data[[i]]
      tempDataLastInventory <- dataLastInventory[[i]]

      ### Selectionne les arbres venant d'être recrutés
      recruits[[i]] <- sqldf("select * from tempData where NumArbre in (select NumArbre from tempData except select TreeFieldNum from tempDataLastInventory)")

      ### Crée un tableau des recrues anormales
      abnormalRecruits[[i]] <- recruits[[i]] %>%
        filter((Circonference < 31.5 | Circonference > 40) & !(idVern >= 500 & idVern < 600))  %>%
        mutate(message = "Recrue dont la circonférence est <31cm ou >40cm", MesuMort = as.logical(MesuMort)) %>%
        select(Libelle, NumCarre, NumArbre, idVern, X, Y, Circonference, MesuMort, message) %>%
        rename(CircActuel = Circonference, StatutActuel = MesuMort)
      ### Jointure entre les données du dernier inventaire et les données d'entrée
      mergedData[[i]] <- inner_join(tempData, tempDataLastInventory, by=c("NumArbre" = "TreeFieldNum"))
      ### Crée une colonne différence entre circ du dernier inventaire et circ de des données d'entrée
      output[[i]] <- mergedData[[i]] %>%
        mutate(diff = Circonference - Circ)

      ### Crée un tableau contenant les arbres différents sur les mêmes coordonnées
      duplicateTrees[[i]] <- mergedData[[i]] %>%
        unite(col=coordinates,X,Y) %>%
        group_by(coordinates) %>%
        filter(n()>1) %>%
        arrange(coordinates) %>%
        separate(coordinates, c("X","Y"), sep="_") %>%
        mutate(message = "Arbres dupliqués", MesuMort = as.logical(MesuMort)) %>%
        select(Libelle, NumCarre, NumArbre, idVern.x, idTree, X, Y, Circ, Circonference, CodeAlive, MesuMort, message) %>%
        rename(idVern = idVern.x , i_arbre = idTree, CircDernierInventaire = Circ, CircActuel = Circonference, StatutDernierInventaire = CodeAlive, StatutActuel = MesuMort)

      ### Crée un dataframe contenant les arbres à croissance anormales
      output[[i]] <- output[[i]] %>%
        filter(diff > (6.5*pi*diffYear[[i]]) | diff < (-2*pi*diffYear[[i]])) %>%
        mutate(message = "Croissance supérieure à -2cm ou 6.5cm de diamètre par an", MesuMort = as.logical(MesuMort)) %>%
        select(Libelle, NumCarre, NumArbre, idVern.x, idTree, X, Y, Circ, Circonference, diff, CodeAlive, MesuMort, message) %>%
        rename(idVern = idVern.x , i_arbre = idTree, CircDernierInventaire = Circ, CircActuel = Circonference, StatutDernierInventaire = CodeAlive, StatutActuel = MesuMort)

    }
    ### Fusionnes la liste des dataframes
    mergedData <- bind_rows(mergedData)

    ### Fusionnes la liste des dataframes
    duplicateTrees <- bind_rows(duplicateTrees)

    ### Crée un dataframe contenant les codes mesures incohérents
    incompatibleMeasCode <- mergedData %>%
      mutate(message="Code mesure incohérent", MesuMort = as.logical(MesuMort)) %>%
      filter(((!idCodeMesure %in% c(0,6,7) & (MesuMort == FALSE))) ||
               ((!idCodeMesure %in% c(0,1,2,3,4)) & (MesuMort == TRUE)) ||
               (idCodeMesure >= 9)
      ) %>%
      select(Libelle, NumCarre, NumArbre, idVern.x, idTree, X, Y, Circ, Circonference, CodeAlive, MesuMort, message) %>%
      rename(idVern = idVern.x , i_arbre = idTree, CircDernierInventaire = Circ, CircActuel = Circonference, StatutDernierInventaire = CodeAlive, StatutActuel = MesuMort)

    ### Crée un dataframe contenant les arbres ressuscités
    resurectedTrees <- mergedData %>%
      mutate(message="Arbre ressucité", MesuMort = as.logical(MesuMort)) %>%
      filter((CodeAlive == FALSE) & (MesuMort == TRUE)) %>%
      select(Libelle, NumCarre, NumArbre, idVern.x, idTree, X, Y, Circ, Circonference, CodeAlive, MesuMort, message) %>%
      rename(idVern = idVern.x , i_arbre = idTree, CircDernierInventaire = Circ, CircActuel = Circonference, StatutDernierInventaire = CodeAlive, StatutActuel = MesuMort)

    ### Crée un dataframe contenant les arbres à circonférence nulle
    badCirc <- mergedData %>%
      mutate(message="Circonférence manquante", MesuMort = as.logical(MesuMort)) %>%
      filter(((Circonference == 0) & (MesuMort == TRUE)) | (is.na(Circonference))
      ) %>%
      select(Libelle, NumCarre, NumArbre, idVern.x, idTree, X, Y, Circ, Circonference, CodeAlive, MesuMort, message) %>%
      rename(idVern = idVern.x , i_arbre = idTree, CircDernierInventaire = Circ, CircActuel = Circonference, StatutDernierInventaire = CodeAlive, StatutActuel = MesuMort)


    ### Concatène tous les dataframes créés précédemments
    output <- bind_rows(output, badCirc, incompatibleMeasCode, resurectedTrees, abnormalRecruits, duplicateTrees)

    ### Exporte le CSV concaténé
    write.csv2(output, "output.csv")
}







