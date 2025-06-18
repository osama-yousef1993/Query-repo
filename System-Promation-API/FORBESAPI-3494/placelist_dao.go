package dao

import (
	"time"

	. "github.com/Forbes-Media/Systems-golang-common-libraries/fdao"
	"github.com/Forbes-Media/fum"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// PlaceListDAO defines the interface for interacting with the PlaceList data in MongoDB.
// It extends the BaseDAO interface and adds a method to retrieve PlaceList documents based on the list URI and year.
// It performs the following steps:
// 1. Inherits basic CRUD operations from BaseDAO for PlaceList entities identified by primitive.ObjectID.
// 2. Adds a custom method `GetAllByListUriAndYear` to query PlaceList records based on the list URI and year.
//
// Methods:
//   - GetAllByListUriAndYear(listUri string, year *int, opts GetAllByListUriAndYearOpts) ([]fum.PlaceList, error):
//     Retrieves all PlaceList records based on the provided list URI and optional year, with additional options defined by opts.
type PlaceListDAO interface {
	BaseDAO[fum.PlaceList, primitive.ObjectID] // Inherits CRUD operations for PlaceList entities
	GetAllByListUriAndYear(listUri string, year *int, opts GetAllByListUriAndYearOpts) ([]fum.PlaceList, error)
}

// Ensure that placeListDAOImpl satisfies the PlaceListDAO interface.
var _ BaseDAO[fum.PlaceList, primitive.ObjectID] = &placeListDAOImpl{}

// placeListDAOImpl is the concrete implementation of the PlaceListDAO interface.
// It embeds the BaseDAOMongoImpl structure to inherit CRUD operations for PlaceList entities.
// It is used to interact with MongoDB collections specific to PlaceList data.
type placeListDAOImpl struct {
	*BaseDAOMongoImpl[fum.PlaceList, primitive.ObjectID] // Embeds the base DAO implementation for MongoDB operations
}

// NewPlaceListDAO creates a new PlaceListDAO instance, which provides access to the PlaceList collection in MongoDB.
// It performs the following steps:
// 1. Initializes the MongoDB client to interact with the "PlaceList" collection in the specified database.
// 2. Creates a new placeListDAOImpl instance by wrapping a BaseDAOMongoImpl with the specified timeout.
// 3. Returns the initialized PlaceListDAO to be used for further database operations.
//
// Parameters:
// - contentsClient: A MongoDB client used to interact with the database containing the PlaceList collection.
//
// Returns:
// - PlaceListDAO: An instance of PlaceListDAO that can be used to perform operations on the "PlaceList" collection.
func NewPlaceListDAO(contentsClient *mongo.Client) PlaceListDAO {
	coll := contentsClient.Database(DbContents).Collection("PlaceList")
	return &placeListDAOImpl{BaseDAOMongoImpl: NewBaseDAOMongoImpl[fum.PlaceList, primitive.ObjectID](coll, 300*time.Second)}
}

// GetAllByListUriAndYear retrieves a list of PlaceList documents from MongoDB based on the list URI, year,
// and additional filtering/sorting/pagination options.
//
// Parameters:
//   - listUri: The URI identifier of the list to filter by.
//   - year: Optional year to filter results (nil ignores the year filter).
//   - opts: Configuration options for filtering, sorting, pagination, and field projection.
//
// Returns:
//   - []fum.PlaceList: A slice of PlaceList documents matching the criteria.
//   - error: An error if the query fails or data cannot be unmarshaled.
//
// Query Logic:
//   - Constructs a base query with `listUri` and `visible: true`.
//   - Adds criteria for year, embargo status (if excluded), filters, multi-select filters, and optional search.
//   - Applies sorting, collation, pagination (skip/limit), and field projection based on `opts`.
func (p *placeListDAOImpl) GetAllByListUriAndYear(listUri string, year *int, opts GetAllByListUriAndYearOpts) ([]fum.PlaceList, error) {
	// Base query: Filter by listUri and ensure documents are visible
	query := bson.M{
		"listUri": listUri,
		"visible": true,
	}

	// Add optional criteria
	AddCriteria(query, NbvPairsToAndCriteria("year", year))            // Year filter (optional)
	AddCriteria(query, embargoCriteriaIfNot(opts.IncludeEmbargo))      // Exclude embargoed unless explicitly included
	AddCriteria(query, NbkCriteria(opts.Filters))                      // Standard filters
	AddCriteria(query, MapToCriteriaInValues(opts.MultiSelectFilters)) // Multi-select filters (e.g., "IN" clauses)

	// Add search criteria only if search columns are specified (even if query is empty)
	if opts.SearchQuery == "" && len(opts.SearchColumns) > 0 {
		AddCriteria(query, searchQueryStringOnColumnsArrayToCriteria(opts.SearchQuery, opts.SearchColumns))
	}

	// Conditionally add null/empty field filtering for sort fields (e.g., hide nulls)
	AddCriteriaIf(query, CsvStringToExistsCriteria(opts.Sort), opts.FilterNullSortFields)

	// Execute query with additional options
	return p.GetAllByQuery(
		query,
		nil,
		QOptProjectIfNotEmptyElseExclude(opts.FieldFilters), // Project/include fields or exclude _id
		QOptCollation(Collation{Locale: "en"}),              // Case-insensitive collation
		QOptSortFromCsv(opts.Sort),                          // Sort order (e.g., "field1,-field2")
		QOptSkipLimit(opts.Start, opts.Limit),               // Pagination (skip/limit)
	)
}
