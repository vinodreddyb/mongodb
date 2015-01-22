package com.vinod.core;

import java.net.UnknownHostException;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteResult;

public class MongoHw3_1 {

	public static void main(String[] args) {
		MongoClient mongo;
		try {
			mongo = new MongoClient(new MongoClientURI("mongodb://localhost:27017/?maxPoolSize=100"));
			/**** Get database ****/
			// if database doesn't exists, MongoDB will create it for you
			DB db = mongo.getDB("school");

			/**** Get collection / table from 'testdb' ****/
			// if collection doesn't exists, MongoDB will create it for you
			DBCollection coll = db.getCollection("students");
			
			//db.students.aggregate( [ { "$unwind": "$scores" }, { '$match': {'scores.type': "homework" } }, { "$group": { '_id':'$_id' , 'minitem': { '$min': "$scores.score" } } } ] )
			
			DBObject unwind = new BasicDBObject("$unwind","$scores");
			
			BasicDBObject match = new BasicDBObject("$match", new BasicDBObject("scores.type","homework"));
			
			DBObject groupFields = new BasicDBObject( "_id", "$_id");
			groupFields.put("minitem", new BasicDBObject( "$min", "$scores.score"));
			DBObject group = new BasicDBObject("$group", groupFields);
			
			//List<DBObject> pipeline = Arrays.asList(unwind, match,group);
			
			AggregationOutput output = coll.aggregate(unwind, match,group);
			
			for (DBObject result : output.results()) {
			    
			    //db.students.update( { '_id': result['_id'] }, { '$pull': { 'scores': { 'score': result['minitem'] } } } )
			    DBObject query = new BasicDBObject("_id",result.get("_id"));
			    DBObject pull = new BasicDBObject("$pull",new BasicDBObject("scores",new BasicDBObject("score",result.get("minitem"))));
			    WriteResult results = coll.update(query, pull);
			    System.out.println(result.get("_id") + "--->" + results.toString());
			}
			
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		

	}

}
