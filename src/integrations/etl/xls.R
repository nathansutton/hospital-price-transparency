library(data.table)
library(stringr)
library(readxl)
options(stringsAsFactors = FALSE)

# must match a reference ontology
concept <- fread("/opt/data/dim/CONCEPT.csv.gz",quote="")
concept <- unique(concept[,c("concept_code","concept_id"),with=F])
names(concept)[1] <- "cpt"

# make a control file to iterate through
files <- list.files("/opt/data/raw/",full.names = TRUE,pattern=".xlsx")
files <- files[order(files)]
control <- data.table(file=files,hospital_id=as.integer(str_replace_all(sapply(str_split(files,"/"),function(x) x[length(x)]),".xlsx","")))

# cross reference the control file with the static dimension table
dim <- fread("/opt/data/dim/hospital.csv")
control <- merge(control,dim[,c("hospital_id","affiliation"),with=F],by="hospital_id")
control <- control[order(hospital_id)]

for(i in 1:nrow(control)){

  # decompress
  system(paste0("gunzip -c ",control$file[i]," > /dev/null 2>&1 &"))
  print(paste0(Sys.time(), " - parsing hospital- ", control$hospital_id[i]))

  dt <- setDT(read_xlsx(control$file[i],sheet=1))

  if(control$hospital_id[i] == 9){
    out <- unique(data.table(
      cpt = dt[['HCPCS']],
      gross = dt[['CHARGE']]
    ))
  } else if (control$hospital_id == 20){
    out <- unique(data.table(
      cpt = dt[['Code']],
      gross = dt[['Inpatient Gross Charge']]
    ))
    out[,cpt := str_replace_all(cpt,"HCPCS ","")]
    out[,cpt := str_replace_all(cpt,"CPT ","")]

    # aggregate the existing long data
    minmax <- dt[!is.na(`Inpatient Negotiated Charge`),list(min=min(`Inpatient Negotiated Charge`),max=max(`Inpatient Negotiated Charge`)),by=c("Code")]
    minmax[,cpt := str_replace_all(Code,"HCPCS ","")]
    minmax[,cpt := str_replace_all(cpt,"CPT ","")]
    minmax[,Code := NULL]
    out <- merge(out,minmax,by="cpt")
  } else if(control$hospital_id[i] %in% c(86,87)){

    dt <- setDT(read_xlsx(control$file[i],sheet=2))

    out <- unique(data.table(
      cpt = dt[['CDM HCPCS']],
      gross = dt[['Standard Price']]
    ))
  } else if(control$hospital_id[i] %in% c(91:102)){

    out <- unique(data.table(
      cpt = dt[['CPT/HCPCS']],
      gross = dt[['Price']],
      min = dt[["Min"]],
      max = dt[["Max"]],
      cash = dt[["Cash Discount"]]
    ))
    out[,cpt:=str_replace_all(cpt,".0","")]
  } else if(control$hospital_id[i] %in% c(104:112)){

    dt <- setDT(read_xlsx(control$file[i],sheet=1))

    out <- unique(data.table(
      cpt = as.character(dt[['CPT/HCPCS/DRG CODE']]),
      gross = dt[['CHARGE']],
      cash = dt[['SELF PAY DISCOUNT RATE']],
      min = dt[['DE-IDENTIFIED INPATIENT MINIMUM']],
      max = dt[['DE-IDENTIFIED INPATIENT MAXIMUM']]
    ))
  } else {
  }

  if ("out" %in% ls()){

    # uniform
    out <- out[!is.na(cpt),]
    out <- out[cpt != '',]
    out <- out[cpt != '*',]

    out <- base::merge(out,concept,by="cpt")
    out[,cpt := NULL]

    long <- melt(out,id.vars="concept_id")
    long[,value := str_replace_all(value, ",", "")]
    long[,value := str_replace_all(value, "[$]", "")]
    long[,value := as.numeric(str_trim(value, "both"))]
    long <- long[!is.na(value) & value > 0,]
    long[,hospital_id := control$hospital_id[i]]
    setcolorder(long,neworder = c(4,1,2,3))
    if(nrow(long) > 0){
      fwrite(long,paste0(file="/opt/data/transformed/",control$hospital_id[i],".csv"),col.names=FALSE)
    }
    rm(out,long)

  }

}
