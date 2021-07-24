//$MATCH

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

// find all the titles where the number of words in the title is just 1

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


//=====================================================================================================
//$PROJECT

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


		
//=====================================================================================================


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


// =======================================================================
//$ GEONEAR
// The collection can have one and only one 2d sphere index
// if using 2d sphere, the distance is returned in meters
// if using legacy coordinated, the distance is returned in radians
//$geoNear must be the first stage in an aggregation pipeline

db.nycFacilities.aggregate(
	[
		{
			$geoNear:{
				near: {
					type: "Point",
					coordinates: [-73.98769766092299, 40.757345233626594]
				},
				distanceField: "distanceFromMongoDB",
				spherical: true
			}
		}
	]
).pretty()	

db.nycFacilities.aggregate(
	[
		{
			$geoNear:{
				near: {
					type: "Point",
					coordinates: [-73.98769766092299, 40.757345233626594]
				},
				distanceField: "distanceFromMongoDB",
				spherical: true,
				query: {type: "Hospital"}
			}
		},
		{
			$limit: 5
		}
	]
).pretty()	


// =======================================================================
//$ Cursor like stages


// count

// MQL
db.solarSystem.find({},{_id: 0, name: 1, numberOfMoons: 1}).count()


//Mongo Aggregate
db.solarSystem.aggregate(
	[
		{
			$match:{
				type: "Terrestrial planet"
			}
		},
		{
			$project:{
				_id : 0,
				name : 1,
				numberOfMoons : 1
			}
		},
		{
			$count : "terrestrial planets count"
		}
	]
)

// skip

// MQL
db.solarSystem.find({},{_id: 0, name: 1, numberOfMoons: 1}).skip(5).pretty()

//Mongo Aggregate
db.solarSystem.aggregate(
	[
		{
			$project:{
				_id : 0,
				name : 1,
				numberOfMoons : 1
			}
		},
		{
			$skip : 1
		}
	]
)

// limit

// MQL
db.solarSystem.find({},{_id: 0, name: 1, numberOfMoons: 1}).limit(5).pretty()

//Mongo Aggregate
db.solarSystem.aggregate(
	[
		{
			$project:{
				_id : 0,
				name : 1,
				numberOfMoons : 1
			}
		},
		{
			$limit : 5
		}
	]
)

// sort
// sort can make use of indexes when placed ahead of project in the pipeline
// sort limited to 100MB by default
// to sort larger datasets, we need to allow diskuse
// MQL

db.solarSystem.find({},{_id: 0, name: 1, numberOfMoons: 1}).sort({numberOfMoons: -1}).pretty()

//Mongo Aggregate
db.solarSystem.aggregate(
	[
		{
			$project:{
				_id : 0,
				name : 1,
				hasMagneticField: 1,
				numberOfMoons : 1
			}
		},
		{
			$sort : {hasMagneticField : -1, numberOfMoons : -1}
		}
	], {allowDiskUse: true}
)

// ============================================================================================
$sample
-- select a random number of documents
-- {$sample : {size: <N>}}
   when
   a. N<= 5% of the documents in source collection AND
   b. Source collection has >= 100 documents AND
   c. $sample is the first stage
   
db.nycFacilities.aggregate(
	[
		{
			$sample: {size: 200}
		}
	]
)   
    
// ============================================================================================    
Exercise:

db.movies.aggregate(
	[
		{
			$match:{
				$and: [
					{"countries": "USA"}
					,{"tomatoes.viewer.rating": {"$gte": 3}}					
				]
			}
		},
		{
			$project:{
				_id: 0
				,title: 1
				,tomatoesViewerRating: "$tomatoes.viewer.rating"
				,cast: 1
				,"num_favs":{
					$size: {
						$setIntersection: [
							"$cast", ["Sandra Bullock", "Tom Hanks", "Julia Roberts", "Kevin Spacey", "George Clooney"]
						]
					}
				}
			}
		},
		{
			$match: {
				"num_favs": {"$ne": 0}
			}
		},
		{
			$sort:{
				num_favs: -1
				,"tomatoes.viewer.rating": -1
				,title: -1
			}
		}
	]
).pretty()
