Sys.setlocale("LC_ALL", "Chinese")

base.dir <- "D:/CODE/R/"

raw.data.path <- paste(base.dir, "raw_data/", sep = "")
raw.data.dirs <- dir(path = paste(base.dir,
                                  "raw_data", sep = ""))
hcs.users.detail <- read.csv(paste(base.dir,
                                   "conf/tenant_info.csv", sep = ""))
flavors.gpus.mapping <- read.csv(paste(base.dir,
                                       "conf/flavor_dev_mapping.csv", 
                                       sep = ""))
result.path <- paste(base.dir, "result/", sep = "")

resource.type.device.mapping <- data.frame(
  RESOURCE_TYPE = c(
    "vgpu_m60_1q",
    "vgpu_m60_2q",
    "vgpu_m60_4q",
    "vgpu_m60_8q",
    "gpu_p100",
    "gpu_v100",
    "gpu_m60_kvm",
    "gpu_p4",
    "gpu_p4_small",
    "fpga_vu9p",
    "fpga_vu9p_common",
    "fpga_vu9p_dev",
    "fpga_vu9p_common_dev"
  ),
  SERVER_DEVICE_NUMBER = c(1, 1, 1, 1,
                           6, 6, 2,
                           2, 8,
                           8, 8,
                           8, 8)
)


QueryUserTypeByTenantID <- function(tenant.id) {
  if (is.na(tenant.id)) {
    return(NA)
  }
  retval <-
    subset(hcs.users.detail,
           as.character(user_tenant_id) == as.character(tenant.id))
  return(retval$user_type)
}

QueryUserNameByTenantID <- function(tenant.id) {
  if (is.na(tenant.id)) {
    return(NA)
  }
  retval <- subset(hcs.users.detail,
                   as.character(user_tenant_id) == as.character(tenant.id))
  return(retval$username)
}

QueryUserLevelByTenantID <- function(tenant.id) {
  if (is.na(tenant.id)) {
    return(NA)
  }
  retval <- subset(hcs.users.detail,
                   as.character(user_tenant_id) == as.character(tenant.id))
  return(retval$user_level)
}

GenerateCustomerType <- function(user.type, user.name) {
  if (is.na(user.type) || is.na(user.name)) {
    return(NA)
  }
  if (user.type == '外部客户') {
    return('外部客户')
  } else if (startsWith(user.name, "op_svc")) {
    return('集成客户')
  } else if (grep('内部', user.type)) {
    return('内部客户')
  } else {
    return('外部客户')
  }
}

WhetherCharges <- function(user.name) {
  if (is.na(user.name)) {
    return(NA)
  }
  if (startsWith(user.name, "op_svc")) {
    return("NO")
  } else{
    return("YES")
  }
}

QueryDeviceNameByFlavor <- function(flavor.name) {
  if (is.na(flavor.name)) {
    return(NA)
  }
  retval <- subset(flavors.gpus.mapping,
                   as.character(FLAVOR_NAME) == as.character(flavor.name))
  return(retval$DEVICE_NAME)
}

QueryDeviceNumberByFlavor <- function(flavor.name) {
  if (is.na(flavor.name)) {
    return(NA)
  }
  retval <- subset(flavors.gpus.mapping,
                   as.character(FLAVOR_NAME) == as.character(flavor.name))
  return(retval$DEVICE_NUMBER)
}

QueryDeviceNumberByResourceType <- function(resource.type) {
  if (is.na(resource.type)) {
    return(NA)
  }
  retval <- subset(
    resource.type.device.mapping,
    as.character(RESOURCE_TYPE) == as.character(resource.type)
  )
  return(retval$SERVER_DEVICE_NUMBER)
}

for (d in raw.data.dirs) {
  hcs.topo.data <-
    read.csv(paste(raw.data.path, d, "/hw.csv", sep = ""))
  if (!file.exists(paste(result.path, d, sep = ""))) {
    dir.create(paste(result.path, d, sep = ""))
  }
  
  # remove the lines which region_name is NA or resource_type is NA
  hcs.topo.data <-
    hcs.topo.data[c(-1) * which(hcs.topo.data$REGION_NAME == ""
                                | hcs.topo.data$RESOURCE_TYPE == "",
                                arr.ind = FALSE), ]
  
  # add CHARGE_MODE_CN column
  charge.mode.en <-
    ifelse(hcs.topo.data$CHARGE_MODE == 0, "Hour", "Month")
  hcs.topo.data$CHARGE_MODE_CN <- charge.mode.en
  
  # add USER_TYPE column
  user.types <- lapply(as.character(hcs.topo.data$TENANT_ID),
                       QueryUserTypeByTenantID)
  user.types <-
    lapply(user.types, function(x)
      ifelse(is.null(x), NA, as.character(x)))
  hcs.topo.data$USER_TYPE <- unlist(user.types)
  
  # add USER_NAME column
  user.names <- lapply(as.character(hcs.topo.data$TENANT_ID),
                       QueryUserNameByTenantID)
  user.names <-
    lapply(user.names, function(x)
      ifelse(is.null(x), NA, as.character(x)))
  hcs.topo.data$USER_NAME <- unlist(user.names)
  
  # add USER_LEVEL column
  user.levels <- lapply(as.character(hcs.topo.data$TENANT_ID),
                        QueryUserLevelByTenantID)
  user.levels <-
    lapply(user.levels, function(x)
      ifelse(is.null(x), NA, as.character(x)))
  hcs.topo.data$USER_LEVEL <- unlist(user.levels)
  
  # add CUSTOMER_TYPE column
  customer.types <- mapply(GenerateCustomerType,
                           hcs.topo.data$USER_TYPE,
                           user.name = hcs.topo.data$USER_NAME)
  customer.types <-
    lapply(customer.types, function(x)
      ifelse(is.null(x), NA, as.character(x)))
  hcs.topo.data$CUSTOMER_TYPE <- unlist(customer.types)
  
  # add WHETHER_CHARGES column
  charges <- lapply(hcs.topo.data$USER_NAME, WhetherCharges)
  charges <-
    lapply(charges, function(x)
      ifelse(is.null(x), NA, as.character(x)))
  hcs.topo.data$WHETHER_CHARGES <- unlist(charges)
  
  # add DEVICE_NAME column
  device.names <- lapply(as.character(hcs.topo.data$FLAVOR),
                         QueryDeviceNameByFlavor)
  device.names <-
    lapply(device.names, function(x)
      ifelse(is.null(x), NA, as.character(x)))
  hcs.topo.data$DEVICE_NAME <- unlist(device.names)
  
  # add DEVICE_NUMBER column
  device.number <- lapply(as.character(hcs.topo.data$FLAVOR),
                          QueryDeviceNumberByFlavor)
  device.number <-
    lapply(device.number, function(x)
      ifelse(is.null(x), NA, as.numeric(x)))
  hcs.topo.data$DEVICE_NUMBER <- unlist(device.number)
  
  # add SERVER_DEVICE_NUMBER column
  server.device.number <-
    lapply(as.character(hcs.topo.data$RESOURCE_TYPE),
           QueryDeviceNumberByResourceType)
  server.device.number <- lapply(server.device.number,
                                 function(x)
                                   ifelse(is.null(x), NA, as.numeric(x)))
  hcs.topo.data$SERVER_DEVICE_NUMBER <- unlist(server.device.number)
  
  # plot the flavor by count.
  flavors <-
   subset(hcs.topo.data$FLAVOR,
          hcs.topo.data$FLAVOR != "" | is.na(hcs.topo.data$FLAVOR))
  plot.data = as.data.frame(table(flavors))
  png(
    filename = paste(result.path, d, "/flavors_statistic.png", sep = ""),
    width = 960,
    height = 640,
    units = "px",
    pointsize = 12,
    bg = "white",
    res = NA
  )
  x.flavors = plot.data$flavors
  y.numbers = plot.data$Freq
  y.max = (max(y.numbers) %/% 100 + 1) * 100
  par(mar = c(9, 4, 4, 4), ps = 12)
  flavor.barplot = barplot(
    y.numbers,
    border = FALSE,
    names.arg = x.flavors,
    col = c('chartreuse3', 'cornflowerblue', 'darkgoldenrod1'),
    ylim = c(0, y.max),
    main = "Flavor Statistic",
    ylab = "VM_NUMBER",
    las = 3
  )
  text(flavor.barplot,
       y.numbers + 5,
       paste("", y.numbers, sep = ""),
       cex =
         1)
  dev.off()
  
  # plot the host by count.
  hosts.infos <-
    subset(
      hcs.topo.data,!grepl("vgpu_m60", hcs.topo.data$RESOURCE_TYPE),
      select = c(REGION_NAME, POD_NAME, RESOURCE_TYPE,
                 HOST_NUMBER)
    )
  hosts.infos <- hosts.infos[!duplicated(hosts.infos),]
  region.hosts.infos <- aggregate(
    hosts.infos$HOST_NUMBER,
    by = list(
      RESOURCE_TYPE = hosts.infos$RESOURCE_TYPE,
      REGION_NAME = hosts.infos$REGION_NAME
    ),
    FUN = sum
  )
  sorted.hosts.infos <-
    region.hosts.infos[order(region.hosts.infos$RESOURCE_TYPE,
                             region.hosts.infos$REGION_NAME),]
  
  hosts.plot.datas <- c()
  for (resource.type in unique(sorted.hosts.infos$RESOURCE_TYPE)) {
    for (region.name in unique(sorted.hosts.infos$REGION_NAME)) {
      retval <- subset(sorted.hosts.infos,
                       RESOURCE_TYPE == resource.type &
                         REGION_NAME == region.name)
      if (is.data.frame(retval) && nrow(retval) == 0) {
        hosts.plot.datas[length(hosts.plot.datas) + 1] <- 0
      } else {
        hosts.plot.datas[length(hosts.plot.datas) + 1] <-
          as.integer(retval$x)
      }
    }
  }
  y_max = (max(hosts.plot.datas) %/% 30 + 1) * 30
  final.hosts.datas <- matrix(hosts.plot.datas,
                              nrow = length(unique(sorted.hosts.infos$REGION_NAME)))
  colnames(final.hosts.datas) = unique(sorted.hosts.infos$RESOURCE_TYPE)
  rownames(final.hosts.datas) = unique(sorted.hosts.infos$REGION_NAME)
  
  png(
    filename = paste(result.path, d, "/hosts_statistic.png", sep = ""),
    width = 960,
    height = 640,
    units = "px",
    pointsize = 12,
    bg = "white",
    res = NA
  )
  par(mar = c(11, 4, 4, 4),
      ps = 12,
      mfrow = c(1, 1))
  
  host_barplot = barplot(
    final.hosts.datas,
    col = c('chartreuse3', 'cornflowerblue', 'darkgoldenrod1'),
    main = "Host Statistic",
    ylab = "HOST_NUMBER",
    ylim = c(0, y_max),
    border = FALSE,
    legend = rownames(final.hosts.datas),
    beside = TRUE,
    las = 2
  )
  text(host_barplot, final.hosts.datas + 1, final.hosts.datas, cex = 1)
  dev.off()
  # write result to a new csv file.
  write.csv(
    hcs.topo.data,
    file = paste(result.path, d, "/result.csv", sep = ""),
    row.names = F
  )
}
