library(dplyr)
library(geosphere)
library(lubridate)

branch <- read.csv("abc_branch.csv")
head(branch)
str(branch)

fleet <- read.csv("fleet.csv")
head(fleet)
str(fleet)

sales <- read.csv("abc_sales.csv")
head(sales)
str(sales)

route <- read.csv("route.csv")
head(route)
str(route)

route.stop <- read.csv("route_stop.csv")
head(route.stop)
str(route.stop)

# creating lagged latitude and longitude variables to compute the distances
# assumption: the distance traveled by the truck is proportional to the distance
# between the two points

route.stop <- route.stop %>% 
    mutate(prev_long = lag(longitude), prev_lat = lag(latitude))

route.stop <- route.stop %>% 
    mutate(ticket = as.integer(as.character(route.stop$ticket)))

route.stop.sales <- merge(route.stop, sales, by = "ticket") %>% 
    select(-customer_number, -sale_date)

route.stop.sales$dist = distHaversine(route.stop.sales[,c(3,4)], route.stop.sales[,c(5,6)], r = 3959)

route.features <- route.stop.sales %>% group_by(route_id) %>%
    summarise(n_tickets = n(), sum_sales = sum(sale_amount), sum_dist = sum(dist))

route.features %>% View()


###############################
# create routes_per_day feature
# this is the number of routes taken by a vehicle per day

route.stop.sales2 <- merge(route.stop, sales, by = "ticket") %>% 
    select(sale_date, route_id)

routes_per_day <- merge(route.stop.sales2, route, by = "route_id") %>% 
    group_by(truck_number, sale_date) %>%
    summarise(n_routes = n_distinct(route_id)) %>% 
    group_by(truck_number) %>%
    summarise(n_days = n(), n_routes = sum(n_routes)) %>% 
    mutate(routes_per_day = n_routes/n_days) %>% 
    select(truck_number, routes_per_day)


###############################
# create sales_per_month feature
# this is the total amount for sales made by a vehicle per month

route.stop.sales3 <- merge(route.stop, sales, by = "ticket") %>% 
    mutate(month_year = paste(month(sale_date), year(sale_date))) %>% 
    select(route_id, month_year, sale_amount)

sales_per_month <- merge(route.stop.sales3, route, by = "route_id") %>% 
    group_by(truck_number, month_year) %>%
    summarise(sum_sale_amt = sum(sale_amount)) %>%
    group_by(truck_number) %>%
    summarise(n_months = n(), sum_sale_amt = sum(sum_sale_amt)) %>% 
    mutate(sales_per_month = sum_sale_amt/n_months) %>% 
    select(truck_number, sales_per_month)


###############################
# create sales_per_year feature
# this is the total amount for sales made by a vehicle per year

route.stop.sales4 <- merge(route.stop, sales, by = "ticket") %>% 
    mutate(year = year(sale_date)) %>% 
    select(route_id, year, sale_amount)

sales_per_year <- merge(route.stop.sales4, route, by = "route_id") %>% 
    group_by(truck_number, year) %>%
    summarise(sum_sale_amt = sum(sale_amount)) %>%
    group_by(truck_number) %>%
    summarise(n_years = n(), sum_sale_amt = sum(sum_sale_amt)) %>% 
    mutate(sales_per_year = sum_sale_amt/n_years) %>% 
    select(truck_number, sales_per_year)


######################################################

route$departure <- strptime(gsub("[A-Z]", " ", route$actual_departure), "%Y-%m-%d %H:%M:%S")
route$arrival <- strptime(gsub("[A-Z]", " ", route$actual_arrival), "%Y-%m-%d %H:%M:%S")

route$time_taken <- as.double(route$arrival-route$departure)

route$hour <- hour(hms(strftime(route$departure, format="%H:%M:%S")))
route$morning <- ifelse(route$hour<=12, 1, 0)

route$month <- month(ymd(strftime(route$departure, format="%Y-%m-%d")))
route$winter <- ifelse(route$month>3 & route$month<11, 0, 1)

route$day <- wday(ymd(strftime(route$departure, format="%Y-%m-%d")))

route$weekend <- ifelse(route$day==1 | route$day==7, 1, 0)

route <- route %>% select(-actual_departure, -actual_arrival, -arrival, 
                          -departure, -driver_number, -month, -day, -hour)

route.features2 <- merge(route.features, route, by = "route_id")


########################################################

route.features3 <- route.features2 %>% group_by(truck_number) %>% 
    summarise(n_routes = n_distinct(route_id),
              n_branches = n_distinct(branch),
              n_tickets = sum(n_tickets, na.rm = TRUE),
              n_morning = sum(morning, na.rm = TRUE)/n_routes,
              n_winter = sum(winter, na.rm = TRUE)/n_routes,
              n_weekend = sum(weekend, na.rm = TRUE)/n_routes,
              total_time = sum(time_taken, na.rm = TRUE),
              total_dist = sum(sum_dist, na.rm = TRUE),
              total_sales = sum(sum_sales, na.rm = TRUE))


route.features3 <- route.features3 %>% mutate(avg_branch_sales = total_sales/n_branches,
                          avg_sales_per_route = total_sales/n_routes,
                          avg_dist_per_route = total_dist/n_routes,
                          avg_time_per_route = total_time/n_routes)


################################3
# Joining all the tables

final.table <- merge(x = fleet, y = route.features3, by = "truck_number", all.x = TRUE) %>%
    merge(y = routes_per_day, by = "truck_number", all.x = TRUE) %>%
    merge(y = sales_per_month, by = "truck_number", all.x = TRUE) %>%
    merge(y = sales_per_year, by = "truck_number", all.x = TRUE)


final.table$empty_weight[final.table$unit_type=='Drywall Boom'] = 40400
final.table$gross_weight[final.table$unit_type=='Drywall Boom'] = 62000
final.table$empty_weight[final.table$unit_type=='Conveyor'] = 25000
final.table$gross_weight[final.table$unit_type=='Conveyor'] = 54000
final.table$empty_weight[final.table$unit_type=='Knuckle Boom'] = 37500
final.table$gross_weight[final.table$unit_type=='Knuckle Boom'] = 62000
final.table$empty_weight[final.table$unit_type=='Rear Boom'] = 37500
final.table$gross_weight[final.table$unit_type=='Rear Boom'] = 62000
final.table$empty_weight[final.table$unit_type=='Crane'] = 43500
final.table$gross_weight[final.table$unit_type=='Crane'] = 54000
final.table$empty_weight[final.table$unit_type=='Flatbed TA'] = 23000
final.table$gross_weight[final.table$unit_type=='Flatbed TA'] = 54000
final.table$empty_weight[final.table$unit_type=='Semi Tractor'] = 25000
final.table$gross_weight[final.table$unit_type=='Semi Tractor'] = 80000
final.table$empty_weight[final.table$unit_type=='Flatbed SA'] = 13000
final.table$gross_weight[final.table$unit_type=='Flatbed SA'] = 25500
final.table$empty_weight[final.table$unit_type=='Box Truck'] = 12900
final.table$gross_weight[final.table$unit_type=='Box Truck'] = 25500
final.table$empty_weight[final.table$unit_type=='Pick-up'] = 5500
final.table$gross_weight[final.table$unit_type=='Pick-up'] = 10000
final.table$empty_weight[final.table$unit_type=='Jobsite Forklift'] = 6000
final.table$gross_weight[final.table$unit_type=='Jobsite Forklift'] = 12000
final.table$empty_weight[final.table$unit_type=='Warehouse Forklift'] = 6000
final.table$gross_weight[final.table$unit_type=='Warehouse Forklift'] = 12000


final.table <- final.table %>% mutate(district = factor(district), 
                                      branch = factor(branch), 
                                      made_delivery = factor(made_delivery),
                                      replace = factor(replace))



####################################################33
# Productivity

# region, branch, district are not significant at all

fit.spm <- lm(sales_per_month ~ . , 
           data = final.table[complete.cases(final.table),
                              -c(1,2,3,4,13,21,22,23,24,25,30)])

summary(fit.spm)

# removing unit type
fit.spm2 <- lm(sales_per_month ~ . , 
              data = scale(final.table[complete.cases(final.table),
                                 -c(1,2,3,4,5,13,21,22,23,24,25,30)]))

summary(fit.spm2)

########################################################

# Random Forest
library(randomForest)
set.seed(1234)
model.rf <- randomForest(sales_per_month ~ . , 
                         data = final.table[complete.cases(final.table),
                                            -c(1,2,3,4,5,13,21,22,23,24,25,30)],
                         mtry =3, ntree = 500, nodesize = 3, importance = TRUE)


importance(model.rf)
varImpPlot(model.rf)

#creates "partial dependence" plots
par(mfrow=c(2,2))
for (i in 1:20)
    partialPlot(model.rf, pred.data=final.table[complete.cases(final.table),
                                                -c(1,2,3,4,5,13,21,22,23,24,25,30)], 
                x.var= names(final.table[complete.cases(final.table),
                                         -c(1,2,3,4,5,13,21,22,23,24,25,30)])[i], 
                xlab= names(final.table[complete.cases(final.table),
                                        -c(1,2,3,4,5,13,21,22,23,24,25,30)])[i], 
                main=NULL)






############## fit 1: avg time per route

fit1 <- lm(avg_time_per_route ~ . , 
           data = final.table[complete.cases(final.table),
                              -c(1,13,18,19,25,26,27)])

names(summary(fit1)$coefficients[,4]<0.05)

# significant features: region, district, branch, unit, allocation charge, 
# insurance charge, model year, repairs, current meter, replace, n_routes, 
# n_branches, n_tickets, 

# removing branches
fit2 <- lm(avg_time_per_route ~ . , data = final.table[complete.cases(final.table),-c(1,4,13,18,19,25,26,27)])
summary(fit2)






###################
# number of trucks by sale date

temp <- merge(route.stop, sales, by = "ticket") %>% 
    select(route_id, sale_date)

temp2 <- merge(temp, route, by = "route_id") %>% 
    group_by(sale_date) %>%
    summarise(n_trucks = n_distinct(truck_number))

plot(temp2)



############## Notes
# lost a lot of vehicles because of merging
# route and route stop
# route_stop and sales
# no valid time stamps


