library(data.table)
library(stringr)
library(jsonlite)
options(stringsAsFactors = FALSE)

# must match a reference ontology
concept <- fread("/opt/data/vocab/CONCEPT.csv.gz",quote="")
concept <- unique(concept[,c("concept_code","concept_id"),with=F])
names(concept)[1] <- "cpt"

# make a control file to iterate through
files <- list.files("/opt/data/raw/",full.names = TRUE,pattern=".json.gz")
files <- files[order(files)]
control <- data.table(file=files,hospital_id=as.integer(str_replace_all(sapply(str_split(files,"/"),function(x) x[length(x)]),".json.gz","")))

# cross reference the control file with the static dimension table
dim <- fread("/opt/data/dim/hospital.csv")
control <- merge(control,dim[,c("hospital_id","affiliation"),with=F],by="hospital_id")
control <- control[order(hospital_id)]

# iterate through each hospital
for(i in 1:nrow(control)){

  # decompress
  system(paste0("gunzip -c ",control$file[i]," > /dev/null 2>&1 &"))
  print(paste0(Sys.time(), " - parsing hospital- ", control$hospital_id[i]))

  # read in the data
  dt <- fromJSON(control$file[i])
  setDT(dt)

  if(control$hospital_id[i] %in% c(22:28)){

    cash <- dt[TabName=="Hosp Discounted Cash Price",]
    cash <- unique(data.table(
      hospital_id = control$hospital_id[i],
      cpt=cash[["Code"]],
      price='cash',
      amount=cash[[" Inpatient Discounted Charge "]]
    ))
    amount=cash[["Inpatient Discounted Charge"]]
    cash[,amount := str_replace_all(amount, ",", "")]
    cash[,amount := str_replace_all(amount, "[$]", "")]
    cash[,amount := as.numeric(str_trim(amount, "both"))]
    cash[,cpt := str_replace_all(toupper(cpt),"[^A-Z0-9]","")]
    cash[,cpt := str_replace_all(toupper(cpt),"CPT","")]
    cash <- merge(cash,concept,by="cpt")
    cash[,cpt := NULL]
    cash <- cash[!is.na(amount) & amount > 0,]
    setcolorder(cash,neworder = c(1,4,2,3))

    maxi <- dt[TabName=="Hosp Deidentified Payor MinMax " & `Min /Max`=="MAX",]
    maxi <- unique(data.table(
      hospital_id = control$hospital_id[i],
      cpt=maxi[["Code"]],
      price = "max",
      amount=maxi[[" Inpatient Negotiated Charge "]]
    ))
    maxi[,amount := str_replace_all(amount, ",", "")]
    maxi[,amount := str_replace_all(amount, "[$]", "")]
    maxi[,amount := as.numeric(str_trim(amount, "both"))]
    maxi[,cpt := str_replace_all(toupper(cpt),"[^A-Z0-9]","")]
    maxi[,cpt := str_replace_all(toupper(cpt),"CPT","")]
    maxi <- merge(maxi,concept,by="cpt")
    maxi[,cpt := NULL]
    maxi <- maxi[!is.na(amount) & amount > 0,]
    setcolorder(maxi,neworder = c(1,4,2,3))

    mini <- dt[TabName=="Hosp Deidentified Payor MinMax " & `Min /Max`=="MIN",]
    mini <- unique(data.table(
      hospital_id = control$hospital_id[i],
      cpt=mini[["Code"]],
      price = "min",
      amount=mini[[" Inpatient Negotiated Charge "]]
    ))
    mini[,amount := str_replace_all(amount, ",", "")]
    mini[,amount := str_replace_all(amount, "[$]", "")]
    mini[,amount := as.numeric(str_trim(amount, "both"))]
    mini[,cpt := str_replace_all(toupper(cpt),"[^A-Z0-9]","")]
    mini[,cpt := str_replace_all(toupper(cpt),"CPT","")]
    mini <- merge(mini,concept,by="cpt")
    mini[,cpt := NULL]
    mini <- mini[!is.na(amount) & amount > 0,]
    setcolorder(mini,neworder = c(1,4,2,3))

    out <- rbindlist(list(mini,maxi,cash))
  }

  # now that we have conformed the data, prepare it for the databases
  if ("out" %in% ls()){

    # clear any cache from the last iteration
    if(nrow(out) > 0){
      fwrite(out,paste0(file="/opt/data/transformed/",control$hospital_id[i],".csv"),col.names=FALSE)
    }
    rm(out)

  }

}
