// mongo "mongodb://cluster0-shard-00-00-jxeqq.mongodb.net:27017,cluster0-shard-00-01-jxeqq.mongodb.net:27017,cluster0-shard-00-02-jxeqq.mongodb.net:27017/aggregations?replicaSet=Cluster0-shard-0" --authenticationDatabase admin --ssl -u m121 -p aggregations --norc
const matchMovies = [
  {
      $match: {
          'imdb.rating': { $gte: 7 },
          genres: { $nin: ['Crime', 'Horror'] },
          rated: { $in: ['PG', 'G'] },
          languages: { $all: ['English', 'Japanese'] },
      },
  },
  {
      $project: {
          _id: 0,
          title: 1,
          rated: 1,
      },
  },
];

const countOneWordTitles = [
  {
      $project: {
          _id: 0,
          title: 1,
          titleWords: { $split: ['$title', ' '] },
      },
  },
  {
      $project: {
          numberOfTitleWords: { $size: '$titleWords' },
      },
  },
  {
      $match: {
          numberOfTitleWords: { $eq: 1 },
      },
  },
];

const mapPipeline = [
  {
      $project: {
          _id: 0,
          cast: 1,
          title: 1,
          directors: 1,
          writers: {
              $map: {
                  input: '$writers',
                  as: 'writer',
                  in: {
                      $arrayElemAt: [
                          {
                              $split: ['$$writer', ' ('],
                          },
                          0,
                      ],
                  },
              },
          },
      },
  },
  {
      $project: {
          title: 1,
          sameInCastAndWriters: { $setIntersection: ['$cast', '$writers', '$directors'] },
      },
  },
  {
      $match: {
          sameInCastAndWriters: { $elemMatch: { $exists: true } },
      },
  },
];
// Stages

const favorites = ['Sandra Bullock', 'Tom Hanks', 'Julia Roberts', 'Kevin Spacey', 'George Clooney'];

const pipeline = [
  {
      $match: {
          'tomatoes.viewer.rating': { $gte: 3 },
      },
  },
  {
      $addFields: {
          favsIntersection: { $setIntersection: ['$cast', favorites] },
      },
  },
  {
      $project: {
          _id: 0,
          title: 1,
          num_favs: {
              $cond: {
                  if: { $isArray: '$favsIntersection' },
                  then: { $size: '$favsIntersection' },
                  else: 0,
              },
          },
      },
  },
  {
      $match: {
          num_favs: { $gte: 1 },
      },
  },
  {
      $sort: {
          num_favs: -1,
      },
  },
];

db.movies.aggregate(pipeline);

// Rescale

const rescalePipeline = [
  {
      $match: {
          languages: { $all: ['English'] },
          'imdb.rating': { $gte: 1 },
          'imdb.votes': { $gte: 1 },
          year: { $gte: 1990 },
      },
  },
  {
      $addFields: {
          normalized_rating: {
              $avg: [
                  '$imdb.rating',
                  {
                      $add: [
                          1,
                          {
                              $multiply: [
                                  9,
                                  {
                                      $divide: [{ $subtract: ['$imdb.votes', 5] }, { $subtract: [1521105, 5] }],
                                  },
                              ],
                          },
                      ],
                  },
              ],
          },
      },
  },
  {
      $sort: {
          normalized_rating: 1,
      },
  },
  {
      $project: {
          title: 1,
          scaled_votes: 1,
      },
  },
];

// Groups and accumulators

const grpPipeline = [
  {
      $match: {
          awards: { $regex: /Won (.*) Oscar/ },
      },
  },
  {
      $group: {
          _id: null,
          highest_rating: {
              $max: '$imdb.rating',
          },
          lowest_rating: {
              $min: '$imdb.rating',
          },
          average_rating: {
              $avg: '$imdb.rating',
          },
          deviation: {
              $stdDevSamp: '$imdb.rating',
          },
      },
  },
];

// Unwind

const unwindPipeline = [
  {
      $match: {
          languages: { $all: ['English'] },
      },
  },
  {
      $project: { _id: 0, cast: 1, 'imdb.rating': 1 },
  },
  {
      $unwind: '$cast',
  },
  {
      $group: {
          _id: '$cast',
          numFilms: { $sum: 1 },
          average: { $avg: '$imdb.rating' },
      },
  },
  {
      $project: {
          numFilms: 1,
          average: {
              $divide: [{ $trunc: { $multiply: ['$average', 10] } }, 10],
          },
      },
  },
  {
      $sort: {
          numFilms: -1,
      },
  },
];
