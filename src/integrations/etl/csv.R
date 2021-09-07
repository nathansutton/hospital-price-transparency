library(data.table)
library(stringr)
library(R.utils)
options(stringsAsFactors = FALSE)

# must match a reference ontology
concept <- fread("/opt/data/dim/CONCEPT.csv.gz",quote="")
concept <- unique(concept[,c("concept_code","concept_id"),with=F])
names(concept)[1] <- "cpt"

# make a control file to iterate through
files <- list.files("/opt/data/raw/",full.names = TRUE,pattern=".csv.gz")
files <- files[order(files)]
control <- data.table(file=files,hospital_id=as.integer(str_replace_all(sapply(str_split(files,"/"),function(x) x[length(x)]),".csv.gz","")))

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
  dt <- fread(control$file[i])

  # hospital specific configuration
  if(control$hospital_id[i] == 1 | control$hospital_id[i] == 10){
    out <- unique(data.table(
      cpt = dt[['CPT/HCPCS Code']],
      gross = dt[['Unit Price']],
      cash = dt[['Cash Discount Price']],
      max = dt[['Maximum Amount']],
      min = dt[['Minimum Amount']]
    ))
  } else if (control$hospital_id[i] == 12){
    out <- unique(data.table(
      cpt = dt[['HCPCS']],
      cash = dt[['Self-Pay Discount']],
      gross = dt[['Gross Chg']],
      max = dt[['Max Allowable']],
      min = dt[['Min Allowable']]
    ))
  } else if (control$hospital_id[i] == 13){
    out <- unique(data.table(
      cpt = dt[['HCPCS']],
      cash = dt[['Self-Pay']],
      gross = dt[['Gross Chg']],
      max = dt[['Max Allowable']],
      min = dt[['Min Allowable']]
    ))
  } else if (control$hospital_id[i] %in% c(2)){
    out <- unique(data.table(
      cpt = dt[['CPT']],
      gross = dt[['CHARGE AMOUNT ($)']]
    ))
  } else if (control$hospital_id[i] %in% c(30,31,34)) {
    out <- unique(data.table(
      cpt = dt[['CPT']],
      gross = dt[['Gross Charge']],
      cash = dt[['Discounted Cash Price']],
      max = dt[['Maximum Negotiated Charge']],
      min = dt[['Minimum Negotiated Charge']]
    ))
  } else if (control$hospital_id[i] %in% c(2)){
    out <- unique(data.table(
      cpt = dt[['Code']],
      min = dt[['Min_Allowable_835']],
      max = dt[['Max_Allowable_835']]
    ))
  } else if (control$hospital_id[i] %in% c(32)){
    out <- unique(data.table(
      cpt = dt[['CPT']],
      gross = dt[['Gross Charges']],
      cash = dt[['Uninsured Discount']],
      min = dt[['Min']],
      max = dt[['Max']]
    ))
  } else if (control$hospital_id[i] %in% c(33)) {
    out <- unique(data.table(
      cpt = dt[['CPT/HCPC Code']],
      gross = dt[['Gross Charge']],
      cash = dt[['Discounted Cash Price']],
      max = dt[['Maximum Negotiated Charge']],
      min = dt[['Minimum Negotiated Charge']]
    ))
  } else if (control$hospital_id[i] %in% c(41)) {
    out <- unique(data.table(
      cpt = dt[[5]], # bad name
      gross = dt[['GROSS CHARGES']],
      cash = dt[['SELF PAY CASH PRICE']],
      max = dt[['MAX NEGOTIATED RATE']],
      min = dt[['MIN NEGOTIATED RATE']]
    ))
  } else if (control$hospital_id[i] %in% c(44,45,46,47)) {
    out <- unique(data.table(
      cpt = dt[['Code (CPT/HCPCS/MS-DRG)']], # not a CPT code
      gross = dt[['Gross Charge']],
      cash = dt[['Uninsured Inpatient Rate']],
      max = dt[['De-identified Max Inpatient Negotiated Rate Across All Payers']],
      min = dt[['De-identified Min Inpatient Negotiated Rate Across All Payers']]
    ))
  } else if (control$hospital_id[i] %in% c(48,49,50)) {
    out <- unique(data.table(
      cpt = dt[['HCPCS/ CPT/NDC Code']], # not a CPT code
      gross = dt[['Charge']],
      cash = dt[['Self Pay']],
      max = dt[['De-identified Maximum']],
      min = dt[['De-identified Minimum']]
    ))
    out$cash <- sapply(str_split(sapply(str_split(out$cash,"up to "),function(x) x[2])," "),function(x) x[1])
  } else if (control$hospital_id[i] %in% c(61,62,63,64)) {
    out <- unique(data.table(
      cpt = dt[['CPT/HCPCS']], # not a CPT code
      gross = dt[['Price']]
    ))
  } else if (control$hospital_id[i] %in% c(65,66,67,68,69,70)) {
    out <- unique(data.table(
      cpt = dt[['HCPCS/CPT Code']], # not a CPT code
      gross = dt[['Gross Charge']]
    ))
    out[["cpt"]] <- str_sub(out[["cpt"]],2,nchar(out[["cpt"]]))
  } else if (control$hospital_id[i] %in% c(73:83)) {
    out <- unique(data.table(
      cpt = dt[['CPT/DRG']], # not a CPT code
      gross = dt[['Gross Charge']],
      cash = dt[['Discounted cash price']],
      max = dt[['De-identified maximum negotiated charge']],
      min = dt[['De-identified minimum negotiated charge']]
    ))
  } else {
  }

  # now that we have conformed the data, prepare it for the databases
  if ("out" %in% ls()){

    # uniform
    out <- out[!is.na(cpt),]
    out <- out[cpt != '',]
    out <- out[cpt != '*',]

    # must match a reference code, usually CPT or HCPCS
    out <- base::merge(out,concept,by="cpt")
    out[,cpt := NULL]

    # melt from wide to long
    long <- melt(out,id.vars="concept_id")
    long[,value := str_replace_all(value, ",", "")]
    long[,value := str_replace_all(value, "[$]", "")]
    long[,value := as.numeric(str_trim(value, "both"))]
    long <- long[!is.na(value) & value > 0,]
    long[,hospital_id := control$hospital_id[i]]
    setcolorder(long,neworder = c(4,1,2,3))

    # write the data to a flatfile for postgres
    if(nrow(long) > 0){
      fwrite(long,paste0(file="/opt/data/transformed/",control$hospital_id[i],".csv"),col.names=FALSE)
    }

    # clear any cache from the last iteration
    rm(out,long)

  }

}
