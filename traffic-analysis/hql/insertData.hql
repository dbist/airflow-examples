SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE TRAFFIC PARTITION (BOROUGH) 
  SELECT `DT`, 
         `TM`, 
         `ZIP CODE`, 
         `LATITUDE`, 
         `LONGITUDE`,
         `LOCATION`,
         `ON STREET NAME`,
         `CROSS STREET NAME`,
         `OFF STREET NAME`,
         `NUMBER OF PERSONS INJURED`,
         `NUMBER OF PERSONS KILLED`,
         `NUMBER OF PEDESTRIANS INJURED`,
         `NUMBER OF PEDESTRIANS KILLED`,
         `NUMBER OF CYCLIST INJURED`,
         `NUMBER OF CYCLIST KILLED`,
         `NUMBER OF MOTORIST INJURED`,
         `NUMBER OF MOTORIST KILLED`,
         `CONTRIBUTING FACTOR VEHICLE 1`,
         `CONTRIBUTING FACTOR VEHICLE 2`,
         `CONTRIBUTING FACTOR VEHICLE 3`,
         `CONTRIBUTING FACTOR VEHICLE 4`,
         `CONTRIBUTING FACTOR VEHICLE 5`,
         `UNIQUE KEY`,
         `VEHICLE TYPE CODE 1`,
         `VEHICLE TYPE CODE 2`,
         `VEHICLE TYPE CODE 3`,
         `VEHICLE TYPE CODE 4`, 
         `VEHICLE TYPE CODE 5`,
         `BOROUGH`
  FROM TRAFFIC_CSV;
