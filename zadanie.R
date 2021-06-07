rm(list = ls())

getwd()

library(tidyverse)
library(DBI)
library(RSQLite)
# install.packages("RPostgres")
library(RPostgres)
library(rstudioapi)

konta <- read.csv("konta.csv")

# zadanie 1

rankAccount <- function(dataFrame, colName, groupName, valueSort, num) {
  
  data <- dataFrame
  
  data %>% 
    filter(!!as.symbol(colName) == groupName) %>% 
    arrange(desc(!!as.symbol(valueSort))) %>%
    top_n(num, !!as.symbol(valueSort))
  
  
  
}


rankAccount(dataFrame = konta, colName = 'occupation', groupName = 'NAUCZYCIEL', valueSort = 'saldo', num = 5)
rankAccount(dataFrame = konta, colName = 'occupation', groupName = 'MURARZ', valueSort = 'saldo', num = 10)

# zadanie 2


rankAccountBigDatatoChunk <- function(filepath, size, colName = 'occupation', groupName = 'NAUCZYCIEL', valueSort = 'saldo', num = 5) {
  
  fileConnection <- file(description = filepath, open = 'r')
  
  data <- read.table(fileConnection, nrows = size,
                     header = T, fill = T, sep = ',')
  
  topn_n_all <- slice(data, 0)
  
  columnsNames <- names(data)
  
  
  repeat {
    
    if(nrow(data) == 0) {
      
      break
      
    }
    
    top_n <- data %>% 
      filter(!!as.symbol(colName) == groupName) %>% 
      arrange(desc(!!as.symbol(valueSort))) %>%
      top_n(num, !!as.symbol(valueSort))
    
    topn_n_all <- rbind(topn_n_all, top_n)
    
    
    data <- read.table(fileConnection, nrows = size,
                       col.names = columnsNames, fill = T, sep = ',')
    
    
    
  }
  
  topn_n_all %>% 
    filter(!!as.symbol(colName) == groupName) %>% 
    arrange(desc(!!as.symbol(valueSort))) %>%
    top_n(num, !!as.symbol(valueSort))
  
}

rankAccountBigDatatoChunk(filepath = 'konta.csv', size = 1000, colName = 'occupation', groupName = 'NAUCZYCIEL', valueSort = 'saldo', num = 5)
rankAccountBigDatatoChunk(filepath = 'konta.csv', size = 1000, colName = 'occupation', groupName = 'MURARZ', valueSort = 'saldo', num = 10)

# zadanie 3


colName = 'occupation'
groupName = 'MURARZ'
valueSort = 'saldo'
num = 10

dbp = "konta"
con = dbConnect(SQLite(), dbp)
tablename = 'konta'

rs <- dbSendQuery(con, paste0("SELECT * FROM ",
                              tablename, ' WHERE ', colName, ' = "', groupName,
                              '" order by ', valueSort, ' desc;'))

fetch(rs, 10)

# account      name   surname age saldo occupation
# 1  2.919401e+25 Elizabeth   Lamanna  54 90249     MURARZ
# 2  1.619400e+25      Mark    Becker  67 89616     MURARZ
# 3  9.310508e+25     Pablo Nordquist  19 87244     MURARZ
# 4  4.812401e+25      John      Cruz  47 86971     MURARZ
# 5  9.720302e+25     Jerry    Kersey  62 86394     MURARZ
# 6  4.219305e+25    Donald  Mccarthy  46 85413     MURARZ
# 7  1.818410e+25     Trish  Scharich  54 84979     MURARZ
# 8  7.920307e+25   Michael     Brown  36 84414     MURARZ
# 9  1.511603e+25     Shaun   Palacio  35 83514     MURARZ
# 10 6.110500e+25     Sofia    Kelley  40 83312     MURARZ



