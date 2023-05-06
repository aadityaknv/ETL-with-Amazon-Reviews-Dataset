from flask import Flask, render_template, request, jsonify
from elasticsearch import Elasticsearch

app = Flask(__name__)

# Connect to Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/searchProductCountsByRating')
def search_product_counts_by_rating():
    return render_template('selectRating.html', title='Search Product Counts by Rating', field_name='star_rating')

@app.route('/searchVoteCountsByRating')
def search_vote_counts_by_rating():
    return render_template('selectRating.html', title='Search Votes Counts by Rating', field_name='star_rating')


@app.route('/selectRating', methods=['GET', 'POST'])
def selectRating():
    title = request.args.get('title')
    field_name = request.args.get('field_name')
    query = request.args.get('query')

    if title == "Search Product Counts by Rating":
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {field_name: query}},
                        {"range": {"star_rating": {"gte": 1, "lte": 5}}}
                    ]
                }
            },
            "size": 0,
            "aggs": {
                "ratings": {
                    "terms": {"field": "star_rating"}
                }
            }
        }

        result = es.search(index='amazonreview_index', body=query)

        buckets = result['aggregations']['ratings']['buckets']
        rating_counts = {str(bucket['key']): bucket['doc_count'] for bucket in buckets}

        return render_template('productCountsByRatingResults.html', title=title, query=query, results=rating_counts)

    if title == "Search Votes Counts by Rating":
        query = {
            "size": 0,
            "aggs": {
                "rating_stats": {
                    "terms": {"field": field_name},
                    "aggs": {
                        "total_helpful_votes": {"sum": {"field": "helpful_votes"}},
                        "total_votes": {"sum": {"field": "total_votes"}}
                    }
                }
            }
        }

        result = es.search(index='amazonreview_index', body=query)
        rating_stats = result['aggregations']['rating_stats']['buckets']

        votes_by_rating = {}
        for bucket in rating_stats:
            rating = bucket['key']
            helpful_votes = bucket['total_helpful_votes']['value']
            total_votes = bucket['total_votes']['value']
            votes_by_rating[rating] = {'helpful_votes': helpful_votes, 'total_votes': total_votes}

        return render_template('voteCountsByRatingResults.html', title=title, query=query, results=votes_by_rating)

@app.route('/reviewHeadlinesWithVotes', methods=['GET', 'POST'])
def review_headlines_with_votes():
    if request.method == 'POST':
        # Get the user's selection of whether they want to see review headlines with or without votes
        votes_option = request.form['votes_option']

        if votes_option == 'with_votes':
            total_votes_filter = {"gt": 0}  # Only show review headlines with votes (total_votes > 0)
        else:
            total_votes_filter = {"lt": 1}  # Only show review headlines without votes

        query = {
            "query": {
                "range": {
                    "total_votes": total_votes_filter
                }
            },
            "size": 10000,
            "_source": ["review_headline"]
        }

        result = es.search(index='amazonreview_index', body=query)

        review_headlines = [hit['_source']['review_headline'] for hit in result['hits']['hits']]

        return render_template('reviewHeadlines.html', review_headlines=review_headlines, votes_option=votes_option)

    # Render the initial form for the user to select whether they want to see review headlines with or without votes
    return render_template('votesForm.html')



if __name__ == '__main__':
    app.run(debug=True)
