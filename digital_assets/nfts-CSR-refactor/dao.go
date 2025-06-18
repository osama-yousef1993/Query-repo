NewNFTsQuery() NFTsQuery                                 // Queries for NFTs Functionality


// returns the local portfolio configuration cache
func (d *dao) NewNFTsQuery() NFTsQuery {
	return &nftsQuery{}
}