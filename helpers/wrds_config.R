
# 1. Encode your WRDS Cloud password
# ==================================
# For detailed instructions, see 
# https://wrds-web.wharton.upenn.edu/wrds/support/Accessing%20and%20Manipulating%20the%20Data/_010Encoding%20your%20WRDS%20Password.cfm
#
# 2. Setup SAS JDBC drivers
# =========================
# Download link:
# https://wrds-web.wharton.upenn.edu/wrds/support/SAS-JDBC-Drivers.zip.cfm
# 
# Once you have downloaded and unzipped the drivers (you will notice that there
# are two drivers), you'll need to place them somewhere permanent on your local
# computer, as RStudio will need to use these drivers each time you connect. 
# WRDS recommends storing them in a dedicated directory in your documents
# folder, like so:
# 
# On a Mac:
# /Users/my_user/Documents/WRDS_Drivers/sas.core.jar
# /Users/my_user/Documents/WRDS_Drivers/sas.intrnet.javatools.jar
# 
# On a PC:
# C:\Users\my_user\Documents\WRDS_Drivers\sas.core.jar
# C:\Users\my_user\Documents\WRDS_Drivers\sas.intrnet.javatools.jar
#
# 3. Configure your WRDS Cloud access
# ===================================
# Uncomment the following template and replace your details in it. Do NOT commit
# the file with your details in it.
#
# wrds_user <- "my_username"
# wrds_pass <- "{SAS002}DBCC5712369DE1C65B19864C1564FB850F398DCF"
# wrds_path <- "C:\\Users\\my_user\\Documents\\WRDS_Drivers\\"
