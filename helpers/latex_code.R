#  Copyright 2013, Satrapade
#  by T. Evgeniou, V. Kapartzianis, N. Nassuphis and D. Spinellis
#  Dual licensed under the MIT or GPL Version 2 licenses.
#  11/2015 functions printLatexTable and printLatexTable2 added by T. Evgeniou and E. Junqu?? de Fortuny


######################################################################################
## Latex commands
######################################################################################

tostars <- function(x) ifelse(abs(x)<=0.01,"**",ifelse(abs(x)<=0.05,"*",ifelse(abs(x)<=0.1,"$^+$","")))

# THIS NEEDS FIXING...
latex_print_number <- function(the_value, digits=2,digitssmall = 6, NAvalue = "-"){ 
  if (!is.numeric(the_value))
    res = the_value
  else {
    if (is.na(the_value)){
      res = NAvalue
    } else {
      res = round(the_value,digits)
      if (round(the_value,digits) == 0 & the_value !=0)
        res = ifelse(abs(the_value) < 10^(-digitssmall), 0, format(the_value,scientific=TRUE,digits=digits,big.mark=","))
    }
  }
  res  
}

#latex_print_number <- function(the_value, digits) ifelse(round(the_value) == the_value, the_value,formatC(the_value,format="f",dig=digits))


# CAN EDIT TO PRINT OUT WHATEVER LATEX WE NEED  
latex_render_data_frame<-function(x,y=NULL, title="",caption="",label="",columns=NULL,bigtitleontop = F,show_rownames=FALSE,red_text="",green_text="",blue_text="",scale=1,digits=2,digitssmall = 6,lastSpecial=F,dorotate=F, tostars_used = NULL, NAvalue = "-",hlinerows = NULL) {
  #Helper functions
  p <- function(...){paste(...)}
  pn <- function(...){paste(...,"\n")} #print newline
  pc <- function(...){paste(...,"&")}  #print column
  
  if(dorotate){
    width = "\\linewidth"
  } else {
    width = "\\textwidth"
  }  
  #Start the table definition
  l <- ""  
  if(dorotate){
    l <- pn(l,"\\clearpage\n")
    l <- pn(l,"\\footnotesize")
    l <- pn(l,"\\begin{landscape}")
  }
  l <- pn(l,"\\begin{table}[!htb]")
  l <- pn(l," \\setlength\\tabcolsep{1pt}")
  l <- pn(l,"\\centering")
  #l <- pn(l,"\\medskip")
  if(bigtitleontop){
    if(title!="")
      if(dorotate) {
        l <- pn(l,"\\captionof{table}{",title,"}")
      }else {
        l <- pn(l,"\\caption{",title,"}")
      }
    if(caption!="") {
      l <- pn(l,"\\parbox{",width,"}")
      l <- pn(l,"{\\scriptsize \\singlespacing",caption,"}\\\\")
      # l <- pn(l,"\\vspace{.2in}")
      
    }
  }
  #l <- pn(l,"\\scalebox{",scale,"} {")
  align <- "l"
  for(i in 1:(ncol(x))) align<-paste(align,"c",sep="")
  l <- pn(l,"\\footnotesize")  
  l <- pn(l,"\\begin{tabular*}{",paste(scale,width),"}{@{\\extracolsep{\\fill} }",align,"}")  
  cat(l)
  
  #### THE TABLE STARTS HERE
  
  cat("\\\\ \n")
  #  cat("\\hline \n")
  if (!is.null(colnames(x))){
    cat("\\hline\n")
    for(i in 1:ncol(x))cat("& ",latexTranslate(colnames(x)[i]))
    cat("\\\\ \n")
    cat("\\hline \n")
  }
  for(i in 1:nrow(x)){
    if(show_rownames)cat(paste("\\textbf{",latexTranslate(rownames(x)[i]),"}",sep="",collapse="")," & ",sep="")
    for(j in 1:ncol(x)){
      the_value<-x[i,j]
      if(is.na(the_value))the_value<-NAvalue
      res<-switch(
        class(the_value),
        numeric= latex_print_number(the_value,digits,digitssmall),
        integer=format(the_value,scientific=FALSE,digits=0,big.mark=","),
        character={
          formatted_value<-latexTranslate(the_value)
          if(the_value %in% red_text)formatted_value<-paste("\\textcolor{red}{",formatted_value,"}")
          if(the_value %in% green_text)formatted_value<-paste("\\textcolor{green}{",formatted_value,"}")
          if(the_value %in% blue_text)formatted_value<-paste("\\textcolor{blue}{",formatted_value,"}")
          formatted_value
        },
        latexTranslate(as.character(the_value))
      )
      if(j>1)cat("& ")
      if (!is.null(tostars_used) & j == 1)
        cat(paste(latex_print_number(res,digits), tostars_used(x[i,3]), sep="", collapse = "")) # THIS IS USED ONLY IF THE MARIX HAS 3 COLUMNS AND THE LAST ONE IS THE p_VALUES. NEEDS FIXING.... TEMPORARY FOR NOW
      else  
        cat(res)
    }
    if (i %in% hlinerows)
      cat("\\\\ \\hline\n")
    else    
      cat("\\\\ \n")
  }
  cat("\\hline")
  cat("\n")
  cat("\\end{tabular*}")
  
  if (!is.null(y)){
    x=y # to repeat the code
    
    l=""
    l <- pn(l,"\\medskip\n")
    #l <- pn(l,"\\scalebox{",scale,"} {")
    align <- "l"
    for(i in 1:ncol(x)) align<-paste(align,"c",sep="")
    l <- pn(l,"\\begin{tabular*}{",paste(scale,width),"}{@{\\extracolsep{\\fill} }",align,"}")
    #l <- pn(l," \\hline\\\\[-2ex]  ")
    
    cat(l)
    
    #### THE TABLE STARTS HERE
    
    cat("\\\\ \n")
    #  cat("\\hline \n")
    for(i in 1:nrow(x)){
      #if(i %% 2 >0) cat("\\rowcolor[gray]{0.95}\n")
      if(show_rownames)cat(paste("\\textbf{",latexTranslate(rownames(x)[i]),"}",sep="",collapse="")," & ",sep="")
      for(j in 1:ncol(x)){
        the_value<-x[i,j]
        if(is.na(the_value))the_value<-"-"
        res<-switch(class(the_value),
                    #numeric=paste("\\textcolor",ifelse(sign(the_value)<0,"{red}","{black}"),"{",formatC(the_value,format="f",dig=digits),"}",sep="",collapse=""),
                    #integer=paste("\\textcolor",ifelse(sign(the_value)<0,"{red}","{black}"),"{",format(the_value,scientific=FALSE,digits=0,big.mark=","),"}",sep="",collapse=""),
                    numeric=formatC(the_value,format="f",dig=digits),
                    integer=format(the_value,scientific=FALSE,digits=0,big.mark=","),
                    character={
                      formatted_value<-latexTranslate(the_value)
                      if(the_value %in% red_text)formatted_value<-paste("\\textcolor{red}{",formatted_value,"}")
                      if(the_value %in% green_text)formatted_value<-paste("\\textcolor{green}{",formatted_value,"}")
                      if(the_value %in% blue_text)formatted_value<-paste("\\textcolor{blue}{",formatted_value,"}")
                      formatted_value
                    },
                    latexTranslate(as.character(the_value))
        )
        if(j>1)cat("& ")
        cat(res)
      }
      cat("\\\\ \n")
    }
    cat("\\hline")
    cat("\n")
    cat("\\end{tabular*}")
    
  }
  
  #############################################
  #End table
  #############################################
  l <- ""
  if(!bigtitleontop){ 
    if(title!="") 
      if(dorotate) {
        l <- pn(l,"\\captionof{table}{",title,"}")
      } else {
        l <- pn(l,"\\caption{",title,"}")
      }
    if(caption!="") {
      l <- pn(l,"\\parbox{",width,"}")
      l <- pn(l,"{\\footnotesize \\singlespacing",caption,"}\\\\")
    }
  }
  if(label!="")
    l <- pn(l,paste("\\label{",label,"}",sep=""))
  l <- pn(l,"\\end{table}")
  if(dorotate) #{
    l <- pn(l,"\\end{landscape}") 
  cat(l)
}


#Work in progress, generic latex printer
printLatexTable2 <- function(df1,df2, title="",title1="", title2="",caption="",bigtitleontop = F, titleontop=F, metric1="CAR",metric2="AR",label="",rowcolumn="",columns=NULL,columns2=NULL,scale=1,lastSpecial=F,dorotate=F,returntext=F, NAvalue = "-",digitsround = 2) {
  #Helper functions
  p <- function(...){paste(...)}
  pn <- function(...){paste(...,"\n")} #print newline
  pc <- function(...){paste(...,"&")}  #print column
  
  if(dorotate){
    width = "\\linewidth"
  } else {
    width = "\\textwidth"
  }  
  #Start the table definition
  l <- ""  
  
  if(dorotate){
    l <- pn(l,"\\clearpage\n")
    l <- pn(l,"\\footnotesize")
    l <- pn(l,"\\begin{landscape}")
  }
  
  l <- pn(l,"\\begin{table}[!htb]")
  l <- pn(l," \\setlength\\tabcolsep{1pt}")
  l <- pn(l,"\\centering")
  #l <- pn(l,"\\medskip")
  
  if(bigtitleontop) {
    if(title!="")
      if(dorotate) {
        l <- pn(l,"\\captionof{table}{",title,"}")
      }else {
        l <- pn(l,"\\caption{",title,"}")
      }
    
    if(caption!="") {
      l <- pn(l,"\\parbox{",width,"}")
      l <- pn(l,"{\\scriptsize \\singlespacing",caption,"}\\\\")
      l <- pn(l,"\\vspace{.2in}")
    }
  }
  l <- pn(l, "\\footnotesize") #make it all a bit more compact
  #############################################
  # TABLE 1 NOW
  #############################################
  #convertprintLatexTable2 input
  tstat  <- df1[,3* (1:(ncol(df1)/3))-1]
  df     <- df1[,3* (1:(ncol(df1)/3))-2]
  # Here we just make sure we fix any p-value issues due to sign
  pval   <- ifelse(df > 0, df1[,3* (1:(ncol(df1)/3))], ifelse(abs(tstat) > 1.3 & df1[,3* (1:(ncol(df1)/3))] > 0.5, 1-df1[,3* (1:(ncol(df1)/3))],df1[,3* (1:(ncol(df1)/3))]))
  
  #l <- pn(l,"\\scalebox{",scale,"} {")
  align <- "l"
  for(i in 1:(3*(ncol(df)))) align<-paste(align,"c",sep="")
  l <- pn(l,"\\begin{tabular*}{",paste(scale,width),"}{@{\\extracolsep{\\fill} }",align,"}")
  
  if(titleontop) 
    if(title1!=""){
      l <- pn(l,"\\toprule")
      l <- pn(l,paste("\\multicolumn{",(3*(ncol(df))),"}{l}{\\textbf{",title1,"}}\\\\"))
      l <- pn(l,"\\midrule")
    }
  
  #l <- pn(l," \\hline\\\\[-2ex]  ")
  #column headers
  l <- pc(l,rowcolumn)
  for(col in 1:ncol(df)) {
    name = ifelse(is.null(columns), colnames(df)[col],columns[col])
    if(col != ncol(df))
      l <- pc(l, "\\multicolumn{2}{c}{",name,"} & {}")
    else 
      l <- pn(l, "\\multicolumn{2}{c}{",name,"}\\\\")
  }
  for(col in 1:ncol(df)) {
    l <- p(l,"\\cline{",3*(col-1)+2,"-",3*(col),"}")
  }
  l <- pn(l,"\\\\[-2ex] ")
  #column subheaders
  l <- pc(l)
  for(col in 1:ncol(df)) {
    l <- pc(l,"{",metric1,"}")
    l <- pc(l,"{$t$-stat}")
    if(col != ncol(df))
      l <- pc(l,"{}")
  }
  l <- pn(l,"\\\\[0.5ex]")
  l <- pn(l,"\\hline")
  
  #Actual table data
  for(row in 1:nrow(df)) {
    l <- pc(l,rownames(df)[row])
    if(! ( lastSpecial && row == nrow(df)) ) {
      for(col in 1:ncol(df)) {
        if (!is.na(df[row,col])){
          #tmpval = latex_print_number(df[row,col],digitsround)
          tmpval = ifelse(round(abs(df[row,col]),2) == 0, round(df[row,col],digitsround), round(df[row,col],2))
          l <- pc(l,paste(tmpval, tostars(pval[row,col]), sep="", collapse = ""))
        }
        else 
          l <- pc(l,paste("", "", sep="", collapse = ""))
        if(col != ncol(df)) {
          if (!is.na(tstat[row,col]))
            #l <- pc(l,round(tstat[row,col],3),sep="")
            l <- pc(l,latex_print_number(tstat[row,col],digitsround),sep="")
          else 
            l <- pc(l,"",sep="")
          l <- pc(l,"{}")#extra spacing between columns
        }else {
          #l <- p(l,round(tstat[row,col],3),"\\\\",sep="")
          l <- p(l,latex_print_number(tstat[row,col],digitsround),"\\\\",sep="")
          if(row == nrow(df))
            l <- p(l,"[0.5ex]")
          l <- pn(l)
        }
      }
    }else { #special last row
      for(col in 1:ncol(df)) {
        if(col != ncol(df)) 
          l <- pc(l,"\\multicolumn{2}{c}{",ifelse(!is.na(df[row,col]), df[row,col],"-"),"}&{}")
        else
          l <- pn(l,"\\multicolumn{2}{c}{",ifelse(!is.na(df[row,col]), df[row,col],"-"),"}\\\\[0.5ex]")
      }
    }
  }
  l <- pn(l,"\\hline\n")
  
  if(!titleontop) 
    if(title1!=""){
      l <- pn(l,"\\toprule")
      l <- pn(l,paste("\\multicolumn{",(3*(ncol(df))),"}{l}{\\textbf{",title1,"}}\\\\"))
      l <- pn(l,"\\midrule")
    }
  
  l <- pn(l,"\\end{tabular*}")
  #l <- pn(l,"}")
  
  ##################################################
  # TABLE 2 NOW
  ##################################################
  l <- pn(l,"\\medskip\n")
  
  #convert input
  #pval   <- df2[,3* (1:(ncol(df2)/3))]
  tstat  <- df2[,3* (1:(ncol(df2)/3))-1]
  df     <- df2[,3* (1:(ncol(df2)/3))-2]
  # Here we just make sure we fix any p-value issues due to sign
  pval   <- ifelse(df > 0, df2[,3* (1:(ncol(df2)/3))], ifelse(abs(tstat) > 1.3 & df2[,3* (1:(ncol(df2)/3))] > 0.5, 1-df2[,3* (1:(ncol(df2)/3))],df2[,3* (1:(ncol(df2)/3))]))
  
  if(!is.null(columns2))
    columns = columns2
  
  #l <- pn(l,"\\scalebox{",scale,"} {")
  align <- "l"
  for(i in 1:(3*(ncol(df)))) align<-paste(align,"c",sep="")
  l <- pn(l,"\\begin{tabular*}{",paste(scale,width),"}{@{\\extracolsep{\\fill} }",align,"}")
  #l <- pn(l," \\hline\\\\[-2ex]  ")
  
  if(titleontop) 
    if(title2!=""){
      l <- pn(l,"\\toprule")
      l <- pn(l,paste("\\multicolumn{",(3*(ncol(df))),"}{l}{\\textbf{",title2,"}}\\\\"))
      l <- pn(l,"\\midrule")
    }
  
  #column headers
  l <- pc(l,rowcolumn)
  for(col in 1:ncol(df)) {
    name = ifelse(is.null(columns), colnames(df)[col],columns[col])
    if(col != ncol(df))
      l <- pc(l, "\\multicolumn{2}{c}{",name,"} & {}")
    else 
      l <- pn(l, "\\multicolumn{2}{c}{",name,"}\\\\")
  }
  for(col in 1:ncol(df)) {
    l <- p(l,"\\cline{",3*(col-1)+2,"-",3*(col),"}")
  }
  l <- pn(l,"\\\\[-2ex] ")
  #column subheaders
  l <- pc(l)
  for(col in 1:ncol(df)) {
    l <- pc(l,"{",metric2,"}")
    l <- pc(l,"{$t$-stat}")
    if(col != ncol(df))
      l <- pc(l,"{}")
  }
  l <- pn(l,"\\\\[0.5ex]")
  l <- pn(l,"\\hline")
  
  #Actual table data
  for(row in 1:nrow(df)) {
    l <- pc(l,rownames(df)[row])
    if(! ( lastSpecial && row == nrow(df)) ) {
      for(col in 1:ncol(df)) {
        tmpval = latex_print_number(df[row,col],digitsround)
        #tmpval = ifelse(round(abs(df[row,col]),2) == 0, round(df[row,col],digitsround), round(df[row,col],2))
        l <- pc(l,paste(tmpval, tostars(pval[row,col]), sep="", collapse = ""))
        if(col != ncol(df)) {
          #l <- pc(l,round(tstat[row,col],3),sep="")
          l <- pc(l,latex_print_number(tstat[row,col],digitsround),sep="")
          l <- pc(l,"{}")#extra spacing between columns
        }else {
          l <- p(l,latex_print_number(tstat[row,col],digitsround),"\\\\",sep="")
          #l <- p(l,round(tstat[row,col],3),"\\\\",sep="")
          if(row == nrow(df))
            l <- p(l,"[0.5ex]")
          l <- pn(l)
        }
      }
    }else { #special last row
      for(col in 1:ncol(df)) {
        if(col != ncol(df)) 
          l <- pc(l,"\\multicolumn{2}{c}{",ifelse(!is.na(df[row,col]), df[row,col],"-"),"}&{}")
        else
          l <- pn(l,"\\multicolumn{2}{c}{",ifelse(!is.na(df[row,col]), df[row,col],"-"),"}\\\\[0.5ex]")
      }
    }
  }
  l <- pn(l,"\\hline\n")
  
  if(!titleontop) 
    if(title2!=""){
      l <- pn(l,"\\toprule")
      l <- pn(l,paste("\\multicolumn{",(3*(ncol(df))),"}{l}{\\textbf{",title2,"}}\\\\"))
      l <- pn(l,"\\midrule")
    }
  
  l <- pn(l,"\\end{tabular*}")
  #l <- pn(l,"}")
  
  #############################################
  #End table
  #############################################
  
  if(!bigtitleontop) {
    if(title!="") 
      if(dorotate) {
        l <- pn(l,"\\captionof{table}{",title,"}")
      }else {
        l <- pn(l,"\\caption{",title,"}")
      }
    
    if(caption!="") {
      l <- pn(l,"\\parbox{",width,"}")
      l <- pn(l,"{\\footnotesize \\singlespacing",caption,"}\\\\")
    }
  }
  if(label!="")
    l <- pn(l,paste("\\label{",label,"}",sep=""))
  l <- pn(l,"\\end{table}")
  
  if(dorotate) #{
    l <- pn(l,"\\end{landscape}") 
  
  if(returntext)
    return(l)
  cat(l)
}  


#Work in progress, generic latex printer
printLatexTable <- function(df, title="",metric="CAR",caption="",label="",titleontop = F, rowcolumn="",columns=NULL,scale=1,close=T,open=T,lastSpecial=F,NAvalue = "-", digitsround = 2) {
  #convert input
  dfini = df
  #pval   <- df[,3* (1:(ncol(df)/3))]
  tstat  <- df[,3* (1:(ncol(df)/3))-1]
  df     <- df[,3* (1:(ncol(df)/3))-2]
  # Here we just make sure we fix any p-value issues due to sign
  pval   <- ifelse(df > 0, dfini[,3* (1:(ncol(dfini)/3))], ifelse(abs(tstat) > 1.3 & dfini[,3* (1:(ncol(dfini)/3))] > 0.5, 1-dfini[,3* (1:(ncol(dfini)/3))],dfini[,3* (1:(ncol(dfini)/3))]))
  
  if (length(metric) ==1)
    metric = rep(metric, ncol(df))
  
  #Design decisions to make
  dorotate <- ncol(df) > 5
  captionontop <- dorotate || (close != T) || (titleontop == T) #singular, non-rotated tables get caption at the bottom
  if(dorotate){
    width = "\\linewidth"
  } else {
    width = "\\textwidth"
  }
  
  #Helper functions
  p <- function(...){paste(...)}
  pn <- function(...){paste(...,"\n")} #print newline
  pc <- function(...){paste(...,"&")}  #print column
  
  #Start the table definition
  l <- ""
  
  #open is a flag that can be used when we're concatenating tables
  if(open) {
    if(dorotate){
      l <- pn(l,"\\begin{landscape}")
      l <- pn(l,"\\clearpage\n")
      l <- pn(l,"\\scriptsize")
    } else {
      l <- pn(l,"\\begin{table}[!htb]")
    }
    l <- pn(l," \\setlength\\tabcolsep{1pt}")
  }
  if(captionontop) {
    if(title!="") {
      if(dorotate) {
        l <- pn(l,"\\captionof{table}{",title,"}")
      }else {
        l <- pn(l,"\\caption{",title,"}")
        l <- pn(l,"\\medskip")
      }
    }
    if(caption!="") {
      l <- pn(l,"\\parbox{",width,"}")
      l <- pn(l,"{\\scriptsize \\singlespacing",caption,"}")
    }
  }
  l <- pn(l,"\\centering")
  l <- pn(l,"\\medskip")
  #l <- pn(l,"\\scalebox{",scale,"} {")
  
  align <- "l"
  for(i in 1:(3*(ncol(df)))) align<-paste(align,"c",sep="")
  l <- pn(l," \\footnotesize")
  l <- pn(l,"\\begin{tabular*}{",paste(scale,width),"}{@{\\extracolsep{\\fill} }",align,"}")
  l <- pn(l," \\hline\\\\[-2ex]  ")
  
  #column headers
  l <- pc(l,"\\textbf{",rowcolumn,"}")
  for(col in 1:ncol(df)) {
    name = ifelse(is.null(columns), colnames(df)[col],columns[col])
    if(col != ncol(df))
      l <- pc(l, "\\multicolumn{2}{c}{",name,"} & {}")
    else 
      l <- pn(l, "\\multicolumn{2}{c}{",name,"}\\\\")
  }
  for(col in 1:ncol(df)) {
    l <- p(l,"\\cline{",3*(col-1)+2,"-",3*(col),"}")
  }
  l <- pn(l,"\\\\[-2ex] ")
  
  #column subheaders
  l <- pc(l)
  for(col in 1:ncol(df)) {
    l <- pc(l,"{",metric[col],"}")
    l <- pc(l,"{$t$-stat}")
    if(col != ncol(df))
      l <- pc(l,"{}")
  }
  l <- pn(l,"\\\\[0.5ex]")
  l <- pn(l,"\\hline")
  
  #Actual table data
  for(row in 1:nrow(df)) {
    l <- pc(l,rownames(df)[row])
    if(! ( lastSpecial && row == nrow(df)) ) {
      for(col in 1:ncol(df)) {
        if (!is.na(df[row,col])){
          tmpval = latex_print_number(df[row,col],digitsround)
          #tmpval = ifelse(round(abs(df[row,col]),2) == 0, round(df[row,col],digitsround), round(df[row,col],2))
          l <- pc(l,paste(tmpval, tostars(pval[row,col]), sep="", collapse = ""))
        }
        else 
          l <- pc(l,paste("", "", sep="", collapse = ""))
        if(col != ncol(df)) {
          if (!is.na(tstat[row,col]))
            #l <- pc(l,round(tstat[row,col],3),sep="")
            l <- pc(l,latex_print_number(tstat[row,col],digitsround),sep="")
          else
            l <- pc(l,"",sep="")
          l <- pc(l,"{}")#extra spacing between columns
        }else {
          if (!is.na(tstat[row,col]))
            #l <- p(l,round(tstat[row,col],3),"\\\\",sep="")
            l <- p(l,latex_print_number(tstat[row,col],digitsround),"\\\\",sep="")
          else
            l <- p(l,"","\\\\",sep="")
          if(row == nrow(df))
            l <- p(l,"[0.5ex]")
          l <- pn(l)
        }
      }
    }else { #special last row
      for(col in 1:ncol(df)) {
          thevalue = latex_print_number(df[row,col])
        if(col != ncol(df)) 
          l <- pc(l,"\\multicolumn{2}{c}{",thevalue,"}&{}")
        else
          l <- pn(l,"\\multicolumn{2}{c}{",thevalue,"}\\\\[0.5ex]")
      }
    }
  }
  l <- pn(l,"\\hline\n")
  l <- pn(l,"\\end{tabular*}")
  #l <- pn(l,"}")
  
  #End table
  if(!captionontop) {
    if(title!="") {
      if(dorotate) {
        l <- pn(l,"\\captionof{table}{",title,"}")
      }else {
        l <- pn(l,"\\caption{",title,"}")
      }
    }
    if(caption!="") {
      l <- pn(l,"\\parbox{",width,"}")
      l <- pn(l,"{\\footnotesize \\singlespacing",caption,"}")
    }
  }
  if(label!="")
    l <- pn(l,paste("\\label{",label,"}",sep=""))
  
  if(close) {
    if(dorotate) {
      l <- pn(l,"\\end{landscape}") 
    } else {
      l <- pn(l,"\\end{table}")
    }
  }
  cat(l)
}


####


print_dataframe_latex<-function(x,title="", caption = ""){
  x1<-xtable(x,format="latex",row.names=FALSE, longtable=TRUE, booktabs=TRUE,digits=0,caption=caption)
  rcol<-tail(ifelse((1:nrow(x1)-1)%%2==0,"\\rowcolor[gray]{1.00} \n","\\rowcolor[gray]{0.95} \n"),-1)
  rndx<-tail(1:nrow(x1)-1,-1)
  title_string<-""
  if(length(title)>0)title_string<-paste0(
    "\\hline \n",
    "\\multicolumn{",ncol(x),"}{c}{",title,"} \\\\ \n"
  )
  print(
    x1,
    floating=FALSE,
    include.rownames = FALSE,
    include.colnames = FALSE,
    tabular.environment = "longtable",
    add.to.row = list(
      pos = as.list(c(0,rndx)),
      command = c(paste(
        title_string,
        "\\hline  \n",
        paste(latexTranslate(colnames(x)), collapse=" & "), "\\\\",
        "\\hline  \n",
        "\\endfirsthead \n", 
        "\\hline \n", 
        paste(latexTranslate(colnames(x)), collapse=" & "), "\\\\",
        "\\hline \n",
        "\\endhead \n", 
        "\\hline \n",
        "\\multicolumn{",ncol(x),"}{r}{\\textit{Continued}} \\ \n", 
        "\\endfoot ",
        "\\endlastfoot \n",
        collapse=" "),rcol)
    )
  )
}

render_latex_pnl_matrix<-function(x1,caption,label="",digits=0,lmargin="-2cm", resize_factor=1,float=TRUE){
  x<-ifelse(is.na(x1),0,x1)
  m_max_x<-max(x[col(x)<ncol(x)])
  m_min_x<-min(x[col(x)<ncol(x)])
  y_max_x<-max(x[,ncol(x)])
  y_min_x<-min(x[,ncol(x)])
  month_pnl_to_latex<-function(xi){
    if(is.na(xi))return("-") #return("\\cellcolor{ black!20 } $\\times$")
    if(xi>0)return(paste("{\\color[rgb]{0,",round((xi/m_max_x)^(1/3),digits=2),",0}",formatC(xi,format="f",dig=digits),"}",sep="",collaplse=""))
    if(xi<0)return(paste("{\\color[rgb]{",round((xi/m_min_x)^(1/3),digits=2),",0,0}",formatC(xi,format="f",dig=digits),"}",sep="",collaplse=""))
    return(formatC(xi,format="f",dig=digits))
  }
  year_pnl_to_latex<-function(xi){
    if(is.na(xi))return("-") #("\\cellcolor{ black!20 } $\\times$")
    if(xi>0)return(paste("{\\color[rgb]{0,",round(xi/y_max_x,digits=2),",0}",formatC(xi,format="f",dig=digits),"}",sep="",collaplse=""))
    if(xi<0)return(paste("{\\color[rgb]{",round(xi/y_min_x,digits=2),",0,0}",formatC(xi,format="f",dig=digits),"}",sep="",collaplse=""))
    return(formatC(xi,format="f",dig=digits))
  }
  cat("\\begin{center} \n")
  if(length(lmargin)>0)cat("\\begin{adjustwidth}{",lmargin,"}{}\n",sep="")
  cat("\\centering\n")
  if(float)cat("\\begin{table}[ht]\n")
  cat(paste(paste("\\scalebox{", resize_factor,sep=""), "}{ \n", sep=""))
  cat("\\begin{tabular}{",paste(paste(rep("r",ncol(x)),collapse=""),"r",sep="",collapse=""),"}\n",sep="")
  cat("\\hline\n")
  for(i in 1:ncol(x))cat("& ",latexTranslate(colnames(x)[i]))
  cat("\\\\ \n")
  cat("\\hline \n")
  for(i in 1:nrow(x)){
    if(i %% 2 >0) cat("\\rowcolor[gray]{0.95}\n")
    cat(latexTranslate(rownames(x)[i])," ")
    for(j in 1:ncol(x)){
      if(j<ncol(x))cat("& ",month_pnl_to_latex(x1[i,j])) else cat("& \\textbf{",year_pnl_to_latex(x1[i,j]),"}")
    }
    cat("\\\\ \n")
  }
  cat("\\hline \n")
  cat("\\end{tabular}\n")
  cat("} \n")
  if(caption!="")if(float){
    cat("\\caption{",latexTranslate(caption),"}\n") 
  } else {
    cat("\\captionof{table}{",latexTranslate(caption),"}\n") 
  }
  if(label!="")
    cat(paste("\\label{",latexTranslate(label),"}\n",sep="")) 
  if(float)cat("\\end{table}")
  if(length(lmargin)>0)cat("\\end{adjustwidth}\n")
  cat("\\end{center}")
}

###


