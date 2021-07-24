$MATCH

db.movies.find({}).count()


db.movies.find(
	{
		"type":"movie"
		,"imdb.rating": {"$gte":7}
		,"genres": {"$not": {"$elemMatch": {"$in":["Horror","Crime"]}}}
		,"rated": {"$in":["G","PG"]}
		,"languages": {"$elemMatch": {"$in":["English","Japanese"]}}
	}
	,{"genres":1,"_id":0,"imdb.rating":1,"type":1,"rated":1,"languages":1}
)

var pipeline = [ 
	{ 
		"$match": {
			"$and": [
				{"type":"movie"}
				,{"imdb.rating": {"$gte":7}}
				,{"genres": {"$not": {"$elemMatch": {"$in":["Horror","Crime"]}}}}
				,{"rated": {"$in":["G","PG"]}}
				,{"languages": {"$elemMatch": {"$in":["English","Japanese"]}}}
			]
		} 
	} 
]

var pipeline = [ 
	{ 
		$match: {
			$and: [
				{"type":"movie"}
				,{"imdb.rating": {"$gte":7}}
				,{"genres": {"$not": {"$elemMatch": {"$in":["Horror","Crime"]}}}}
				,{"rated": {"$in":["G","PG"]}}
				,{"languages": {"$elemMatch": {"$in":["English","Japanese"]}}}
			]
		}
	},
	{
		$project:{
			title: 1,
			rated: 1,
			_id: 0
		}
	}	
]

db.movies.aggregate([
   {
	  $match:{
			 "imdb.rating":{$gte:7},
			 "genres"     :{$nin:["Crime","Horror"]},
			 "rated"      :{$in:["PG","G"]},
			 "languages"  :{$all:["English","Japanese"]}
			}
   },
   {
	  $project:{
			  title:1 , rated:1, _id:0
			 }
   }	   
]);

var pipeline = [
   {
	  $match:{
			 "imdb.rating":{$gte:7},
			 "genres"     :{$nin:["Crime","Horror"]},
			 "rated"      :{$in:["PG","G"]},
			 "languages"  :{$all:["English","Japanese"]}
			}
   },
   {
	  $project:{
			  title:1 , rated:1, _id:0
			 }
   }	   
]

-- find all the titles where the number of words in the title is just 1

db.movies.aggregate(
	[
		{
			$project:{
			_id: 0,
			"sizeTitle": {$size: {$split: ["$title", " "]}}
			}
		},
		{
			$match: {
				"sizeTitle": 1
			}
		}
	]
).itcount()


=====================================================================================================
$PROJECT

db.solarSystem.aggregate(
	[
		{
			$project: {_id: 0, name: 1, surfaceGravity: "$gravity.value"}
		}
	]
)

db.solarSystem.aggregate([{
	$project: {
		_id: 0, 
		name: 1,
		gravityRatio: { $divide: [ "$gravity.value", 9.8 ] } }}])

db.solarSystem.aggregate([{
	$project: {
		_id: 0, 
		name: 1,
		gravityRatio: { $divide: [ "$gravity.value", 9.8 ] },
		myWeight: { $multiply: [ { $divide: [ "$gravity.value", 9.8 ] },77 ] }	
}}])


		
=====================================================================================================


db.movies.find({"_id" : ObjectId("573a1390f29313caabcd4217")},{cast:1,directors:1,writers:1}).pretty()	

db.movies.aggregate(
	[
		{ 
			$match: { cast: { $elemMatch: { $exists: true } } }
		},
		{ 
			$match: { directors: { $elemMatch: { $exists: true } } }
		},
		{ 
			$match: { writers: { $elemMatch: { $exists: true } } }
		},
		{
			$project: {
				writers:1,
				writers_split: {
				  $map: {
					input: "$writers",
					as: "writer",
					in: {
					  $arrayElemAt: [
						{
						  $split: [ "$$writer", " (" ]
						},
						0
					  ]
					}
				  }
				},
				cast:1,
				cast_split: {
				  $map: {
					input: "$cast",
					as: "cast",
					in: {
					  $arrayElemAt: [
						{
						  $split: [ "$$cast", " (" ]
						},
						0
					  ]
					}
				  }
				},
				directors:1,
				directors_split: {
				  $map: {
					input: "$directors",
					as: "directors",
					in: {
					  $arrayElemAt: [
						{
						  $split: [ "$$directors", " (" ]
						},
						0
					  ]
					}
				  }
				}
			}
		},
		{
			$project:{
				"commonInAll": {$setIntersection: ["$writers_split", "$directors_split", "$cast_split"]}
			}
		},
		{
			$project:{
				"commonInAll":1,
				"sizeValue": {$size: "$commonInAll"}
			}
		},
		{
			$match: {
				sizeValue : {"$ne":0}
			}
		}
	]
).itcount()

.itcount()
		
		
		

db.movies.aggregate(
	[
		{
			$project: {
				writers:1,
				writers_split: {
				  $map: {
					input: "$writers",
					as: "writer",
					in: {
					  $arrayElemAt: [
						{
						  $split: [ "$$writer", " (" ]
						},
						0
					  ]
					}
				  }
				}
			}
		}
	]
)		

