const express = require("express");
const app = express();

const mysql = require('mysql2');
const pool = mysql.createPool({
    connectionLimit: 10,
    host : 'host',
    user : 'user',
    password : 'password',
    database : 'keyspace',
    dateStrings: true
});
 
app.get("/", (request, response) => {
  //response.end(`<h1>This is the first homework from the course Big Data Processing 2021</h1>`)
});

//Return all reviews for specified `product_id`
//Return all reviews for specified `product_id` with given `star_rating`
app.route('/reviews/products/:product_id')
  .get(function(request, response, next) {
    let id = request.params["product_id"];
    let rate = request.query.star_rating;

    if(rate == undefined) {
      pool.query(`
      SELECT review_headline, review_body 
      FROM reviews 
      WHERE product_id = '${id}'`, 
      function(err, results) {
          if(err) {
              console.log("ERROR\n");
              console.log(err);
          }   
          response.send(results)
      });

    } else{
      pool.query(`
      SELECT review_headline, review_body, star_rating 
      FROM reviews 
      WHERE product_id=${id} AND star_rating=${rate}`, 
      function(err, results) {
        if(err) {
            console.log("ERROR\n");
            console.log(err);
        }   
        response.send(results)
    });
    }
  });

//Return all reviews for specified `customer_id`
app.route('/reviews/customers/:customer_id')
  .get(function(request, response, next) {
    let id = request.params["customer_id"];
    pool.query(`
    SELECT review_headline, review_body 
    FROM reviews 
    WHERE customer_id=${id}`, 
    function(err, results) {
        if(err) {
            console.log("ERROR\n");
            console.log(err);
        }   
        response.send(results)
    });
  });

//Return N most reviewed items (by # of reviews) for a given period of time
app.route('/reviews/popular')
  .get(function(request,response,next) {
    let n = request.query.n;
    let date = request.query.date;


    pool.query(`
    SELECT product_title, Products.product_id, reviews_amount
    FROM 
	    (SELECT product_id, COUNT(review_id) AS reviews_amount 
      FROM 
        (SELECT * 
        FROM reviews
        WHERE review_date = '${date}') AS DateTable
      GROUP BY product_id) AS CountReviews, amazon_books_reviews.products AS Products
    WHERE Products.product_id = CountReviews.product_id
    ORDER BY reviews_amount DESC
    LIMIT ${ n == undefined ? 1 : n}`, 
    function(err, results) {
      if(err) {
          console.log("ERROR\n");
          console.log(err);
      }   
      response.send(results)
  });
  })

//Return N most productive customers (by # of reviews written for verified purchases) for a given period
app.route('/customers/productive')
  .get(function(request,response,next) {
    let n = request.query.n;
    let date = request.query.date;

    pool.query(`
    SELECT customer_id, COUNT(review_id) AS reviews_amount
    FROM 
      (SELECT *
      FROM reviews
      WHERE review_date='${date}') AS DateTable
    WHERE verified_purchase='Y'
    GROUP BY customer_id
    ORDER BY reviews_amount DESC
    LIMIT ${ n == undefined ? 1 : n};`, 
    function(err, results) {
      if(err) {
          console.log("ERROR\n");
          console.log(err);
      }   
      response.send(results)
  });
  })

//Return N best products by fraction of 5-star reviews (among products that have >100 of verified reviews)
app.route('/rating')
  .get(function(request,response,next) {
    let n = request.query.n;
    let limit = 100 //among products that have >100 of verified reviews

    pool.query(`
    SELECT products.product_title, X.product_rating
    FROM
      (SELECT product_id, reviews_amount, product_rating
      FROM 
        (SELECT product_id, AVG(star_rating) AS product_rating, COUNT(star_rating) AS reviews_amount
        FROM
          (SELECT product_id, star_rating, verified_purchase
                FROM reviews
                WHERE verified_purchase = 'Y') AS VerifiedReviews
        GROUP BY product_id
        ORDER BY product_rating DESC) AS CountRating
      WHERE reviews_amount > ${limit}) AS X, products
    WHERE X.product_id = products.product_id
    LIMIT ${ n == undefined ? 1 : n};`, 
    function(err, results) {
      if(err) {
          console.log("ERROR\n");
          console.log(err);
      }   
      response.send(results)
  });
  })

//Return N most productive “haters” (by # of 1- or 2-star reviews) for a given period
app.route('/haters')
  .get(function(request,response,next) {
    let n = request.query.n;
    let date = request.query.date;

    pool.query(`
    SELECT customer_id, COUNT(star_rating) AS negative_reviews_amount
    FROM 
      (SELECT *
      FROM reviews
      WHERE review_date='${date}') AS DateTable
    WHERE star_rating = '1' OR star_rating = '2'
    GROUP BY customer_id
    ORDER BY negative_reviews_amount DESC
    LIMIT ${ n == undefined ? 1 : n};`, 
    function(err, results) {
      if(err) {
          console.log("ERROR\n");
          console.log(err);
      }   
      response.send(results)
  });
  })

//Return N most productive “backers” (by # of 4- or 5-star reviews) for a given period
app.route('/backers')
  .get(function(request,response,next) {
    let n = request.query.n;
    let date = request.query.date;

    pool.query(`SELECT customer_id, COUNT(star_rating) AS positive_reviews_amount
    FROM 
      (SELECT *
      FROM reviews
      WHERE review_date='${date}') AS DateTable
    WHERE star_rating = '4' OR star_rating = '5'
    GROUP BY customer_id
    ORDER BY positive_reviews_amount DESC
    LIMIT ${ n == undefined ? 1 : n};`, function(err, results) {
      if(err) {
          console.log("ERROR\n");
          console.log(err);
      }   
      response.send(results)
  });
  })


app.set('port', process.env.PORT || 3306);
app.listen(3306);