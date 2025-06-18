package repository

import (
	"context"

	"github.com/Forbes-Media/fda-coin-paprika-ingestion/datastruct"
)

type ChangeLog interface {
	UpdateChangeLogCoins(ctx context.Context, changeLog []datastruct.ChangeLog) error
}

type changeLog struct {
}

func (c *changeLog) UpdateChangeLogCoins(ctx context.Context, changeLog []datastruct.ChangeLog) error {
	return nil
}
