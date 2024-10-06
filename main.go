package main

import (
	"context"
	"fmt"
	"strings"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
	"sync"
	
)

type cache struct {
	Vquery PaginatedResult
	expiry	time.Time
}
var (
	query_cache = make(map[string]cache)
	mu      sync.Mutex)


type FilterCriteria struct {
	Field string
	Value interface{}
}

type SortCriteria struct {
	Field string
	Direction string // "ASC" or "DESC"
}

type PaginatedResult struct {
	Data  []map[string]interface{}
	Total int64
}

func FetchPaginatedData(
	pool *pgxpool.Pool,
	tableName string,
	filterBy []FilterCriteria,
	offset int,
	limit int,
	sortBy []SortCriteria,
) (PaginatedResult, error) {

	mu.Lock()
	defer mu.Unlock()

	var result PaginatedResult
	query := fmt.Sprintf("SELECT * FROM %s", tableName)
	var conditions []string
	var arguments []interface{}
	for i, filter := range filterBy{
		conditions = append(conditions, fmt.Sprintf("%s = $%d", filter.Field, i + 1))
		arguments = append(arguments, filter.Value)	
	}

	if (len(conditions) > 0){
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	if (len(sortBy) > 0){
		var order []string
		for _, sort := range sortBy{
			order = append(order, fmt.Sprintf("%s %s", sort.Field, sort.Direction)) 
		}
		query += " ORDER BY " + strings.Join(order, ", ")
	}

	query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", len(arguments) + 1, len(arguments) + 2)
	arguments = append(arguments, limit, offset)

	if val, found := query_cache[query]; found && time.Now().Before(val.expiry){
		return val.Vquery, nil
	}

	rows, err := pool.Query(context.Background(), query, arguments...)
	if err != nil{
		return result, err
	}
	defer rows.Close()

	for (rows.Next()){
		value_row := make(map[string]interface{})
		if err := rows.Scan(&value_row); err != nil{
			return result, err
		}
		result.Data = append(result.Data, value_row)
	}
	CountRow := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	if len(conditions) > 0{
		CountRow += " WHERE " + strings.Join(conditions, " AND ")
	}
	err = pool.QueryRow(context.Background(), CountRow, arguments[:len(arguments) - 2]...).Scan(&result.Total)
	if err != nil{
		return result, err
	}

	query_cache[query] = cache{
		Vquery : result,
		expiry : time.Now().Add(24 * time.Hour),
	}
	return result, nil	
}

/*
func main() {
	// Remplacez par votre chaîne de connexion
	connString := "postgres://postgres:mdp@localhost:code/base"
	pool, err := pgxpool.New(context.Background(), connString)
	if err != nil {
			fmt.Println("Unable to connect to database:", err)
			return
	}
	defer pool.Close()

	// Exemple d'utilisation de FetchPaginatedData
	filter := []FilterCriteria{{Field: "id", Value: 4}}
	sort := []SortCriteria{{Field: "id", Direction: "ASC"}}
	result, err := FetchPaginatedData(pool, "orders", filter, 0, 10, sort)
	sort = []SortCriteria{{Field: "id", Direction: "DESC"}}
	result, err = FetchPaginatedData(pool, "orders", filter, 0, 10, sort)
	if err != nil {
			fmt.Println("Error fetching data:", err)
			return
	}
	fmt.Println("Data:", result.Data)
	fmt.Println("Total:", result.Total)
}
*/