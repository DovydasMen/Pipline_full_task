# Pipline_full_task

NBA prospects draft program according to the stats.
Create docker conteiner with whole new database and collection. Install everything what needs to achieve this task into virtual enviroment.
Crated db should have validation rules. Key arguments should include(name, surname, age, Points, Rebounds, steals, blocks, fouls commited, fouls achieved, +/-, healthy/injured, nationality, current team)
If possible make testcases for most of the project with unittest program.

Tasks:
1.Filter all pleayer which are healthy.
2.Sort all players according to Points in ascending order.
3.Get all players info ignoring nationality and current team.
4.Kauno Zalgiris are searching the best scorer. Get the best player according to points. In this list players can only be under age of 20, healthy and +/- coefficient more > 19.
5.Vilniaus rytas are searching the bes possible guard. Get the best player according to +/-. Expecting that this player avareging more then 8 rebounds or 2 blocks per game. Age can be from 20 to 28. Best mach: year - 22 points <8, rebounds 8-10, steals atleast 1, blocks more than 2, fouls achieved 3. Ignore all other params. 

Work flow:
setting github repository, creating branches. Set up virtual enviroment for the whole project.During project installing all needed libraries. Creating locally docker conteiner. Creating database, collection, aplaying validation rules.Using our created random values generator, we are creating imaginary players with stats, stats should be creating accordingly to validation rules. Creating diffrent piplines in diffrent branches(After we are going to merge everything into stagging). 
