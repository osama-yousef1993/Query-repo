package services

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Forbes-Media/crypto-backend-api/dto"
	"github.com/Forbes-Media/crypto-backend-api/repository/common"
	"github.com/Forbes-Media/go-tools/log"
	ethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/golang-jwt/jwt"
	"github.com/magiclabs/magic-admin-go"
	"github.com/magiclabs/magic-admin-go/client"
	"github.com/magiclabs/magic-admin-go/token"
	"go.opentelemetry.io/otel/codes"
)

var (
	JWT_privateKey       string
	JWT_publicKey        string
	SIGNATURE_privateKey string
	SIGNATURE_publicKey  string
	hmacSecret           string
	magicClient          *client.API
	magic_secret         string
)

func init() {

	privk := os.Getenv("CRYPTOLOGIN_JWT_PRIVATE_KEY")
	pubk := os.Getenv("CRYPTOLOGIN_JWT_PUBLIC_KEY")
	sigprivk := os.Getenv("CRYPTOLOGIN_JWT_PRIVATE_SIGNATURE_KEY")
	sigpubk := os.Getenv("CRYPTOLOGIN_JWT_PUBLIC_SIGNATURE_KEY")
	hmacSecret = os.Getenv("CRYPTOLOGIN_HMAC_KEY")
	magic_secret = os.Getenv("MAGIC_SDK_SECRET") // magic secret key

	var err error
	//if our environment variables are not empty continue
	if privk != "" && pubk != "" && hmacSecret != "" && sigprivk != "" && sigpubk != "" {
		JWT_privateKey, err = common.DecodeBase64ToString(privk)
		if err != nil {
			panic("private key is not valid ")
		}

		JWT_publicKey, err = common.DecodeBase64ToString(pubk)
		if err != nil {
			panic("public key is not valid")
		}
		SIGNATURE_privateKey, err = common.DecodeBase64ToString(sigprivk)
		if err != nil {
			panic("private key is not valid ")
		}

		SIGNATURE_publicKey, err = common.DecodeBase64ToString(sigpubk)
		if err != nil {
			panic("public key is not valid")
		}
	} else {
		panic("could not load environment variables")
	}

	//Generate magic client if there is an error panic
	magicClient, err = client.New(magic_secret, magic.NewDefaultClient())
	if err != nil {
		panic("could not load magicSDK")
	}
}

// Token Manager Service is responsible for generating and parsing JWT tokens
type TokenManagerService interface {
	GenerateJWT(context.Context, dto.JWT) (string, error)                               // generates a jwt string
	ParseJWT(context.Context, string) (*dto.JWT, error)                                 // parses a jwt string into a dto.jwt object
	VerifyHMAC(context.Context, *http.Request) (bool, error)                            //takes a string computes the hmac signature based on a secret
	ValidateDID(context.Context, string) (dto.MagicDIDValidationResults, error)         // Performs Validation on DID token that was provided by magic
	ValidateTokenFormat(context.Context, string) (dto.MagicDIDValidationResults, error) // Performs Validation on Bearer Token that contains Address and Signature
}

// a sturct that implements the TokenManagerService Interface
type tokenManagerService struct {
	privateKey          string // private pkcs1 pem key
	publicKey           string // public pkcs1 pem key
	hmacSecret          string // public pkcs1 pem key
	signaturePrivateKey string // private signature pem key
	signaturePublicKey  string // public signature pem key
}

// creates a new tokenManagesrService
func NewTokenManagerService() TokenManagerService {
	return &tokenManagerService{
		privateKey:          JWT_privateKey,
		publicKey:           JWT_publicKey,
		hmacSecret:          hmacSecret,
		signaturePrivateKey: SIGNATURE_privateKey,
		signaturePublicKey:  SIGNATURE_publicKey,
	}

}

// Verify HMAC reads in a request body. And verifies an hmac was passed in and it was valid
func (t *tokenManagerService) VerifyHMAC(ctx context.Context, r *http.Request) (bool, error) {

	span, labels := common.GenerateSpan("tokenManagerService.VerifyHMAC", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "tokenManagerService.VerifyHMAC"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "tokenManagerService.VerifyHMAC"))

	clientHMAC := r.Header.Get("HMAC")
	var calculatedHMAC string
	// create new hmac
	h := hmac.New(sha256.New, []byte(t.hmacSecret))
	h.Write([]byte(r.Method))       // Add Method
	h.Write([]byte(r.URL.Path))     // Add Path
	h.Write([]byte(r.URL.RawQuery)) // Add raw query

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.EndTime("tokenManagerService.VerifyHMAC", startTime, nil)
		span.SetStatus(codes.Error, err.Error())
		return false, err
	}
	h.Write([]byte(string(body))) // Get body
	calculatedHMAC = hex.EncodeToString(h.Sum(nil))

	if calculatedHMAC == clientHMAC {
		return true, nil
	}

	log.EndTime("tokenManagerService.VerifyHMAC", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return false, errors.New("client not verified")

}

// Generates a ne jwt token string based of of a dto.jwt.
func (t *tokenManagerService) GenerateJWT(ctx context.Context, details dto.JWT) (string, error) {
	span, labels := common.GenerateSpan("tokenManagerService.GenerateJWT", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "tokenManagerService.GenerateJWT"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "tokenManagerService.GenerateJWT"))

	var (
		err         error
		claims      jwt.MapClaims // jwt claims map
		token       *jwt.Token    // token generated by jwt.NewWithClaims
		tokenString string        // token in string format generated by token.SignedString(privateKey)
	)

	//load private pem key
	privateKey, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(t.privateKey))
	if err != nil {
		log.EndTime("tokenManagerService.ParseJWT", startTime, nil)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}

	// create the payload mapping
	claims = jwt.MapClaims{
		"sub":    details.SUB,
		"iat":    details.IAT,
		"iss":    details.ISS, // Subject (a unique identifier for the token)
		"grants": details.Grants,
	}

	// Create a new token and sign it with the private key
	token = jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tokenString, err = token.SignedString(privateKey)
	if err != nil {
		fmt.Println("Error creating token:", err)
		log.EndTime("tokenManagerService.ParseJWT", startTime, nil)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}
	t.ParseJWT(ctx, tokenString)
	log.EndTime("tokenManagerService.ParseJWT", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return tokenString, nil

}

// Verifies if the a JWT is valid based on our publicv key
// If it not valid we raise an error stating the signature is not valid
// otherwise we pars the object into a dto.JWT and retrun it
func (t *tokenManagerService) ParseJWT(ctx context.Context, tokenString string) (*dto.JWT, error) {
	// Read the private key from the file
	span, labels := common.GenerateSpan("tokenManagerService.ParseJWT", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "tokenManagerService.ParseJWT"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "tokenManagerService.ParseJWT"))
	var (
		unparsedData dto.JWT        // object we are parsing our object to
		err          error          // error to return
		publicKey    *rsa.PublicKey //rsa public key generated by  jwt.ParseRSAPublicKeyFromPEM
		token        *jwt.Token     // token generated by jwt.Parse
	)
	//decstr, err := common.DecodeBase64ToString("LS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS0NCk1JSUJJakFOQmdrcWhraUc5dzBCQVFFRkFBT0NBUThBTUlJQkNnS0NBUUVBMFd5ZTVFMGk0eC9DMVk2WE4wcGcNClRRZ1ZlUWNzSUVPelFGQ3dadjc3R05PK1ZXcHg5S2JBbTFRSXBqQi9ZNjcrdUVqVlJIZjI0eENHbEVkM0NlZTYNCmJpaWY0a1NWZHI5byt3R3AvcVZISXBMYVVEWlRER3pEQ0xvRUZZc2FnVVlWWUVqcm9wRmlNeG05UUZQVG1nZmENCnkvZVYwT1ZTam5tRUhDYjVGZFM5RXZDbXJ4Z1paOFZoWjhLTDUvSWNjaDJSK0NMSjlIcUFtZnEyaXgyanVvdHoNCnRCYUFpMmxvVlBTSUU2MUU5SHdFWjJHdW1wbm5aUzZUTkRLVi8wUDBEbXpWK2JiTVFYTTI4TlQwQlhGL1lNNHgNCmJPNDV5Q0o3ZW41ZFJOYTZRbC9BR1piS05UelhDU2w0S3VacEpwVTdTakFTZWlkUTZ5NHhmUjUwZEQwM0ozWHUNCmd3SURBUUFCDQotLS0tLUVORCBQVUJMSUMgS0VZLS0tLS0NCg==")

	publicKey, err = jwt.ParseRSAPublicKeyFromPEM([]byte(t.publicKey))
	if err != nil {
		log.Alert(err.Error())
		log.EndTime("tokenManagerService.ParseJWT", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	token, err = jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		return publicKey, nil
	})

	if token.Valid {

		// assert the claims are of type mapclaims
		if claims, ok := token.Claims.(jwt.MapClaims); ok {

			// marshal claims to a json object
			claimsJSON, err := json.Marshal(claims)
			if err != nil {
				log.Alert(err.Error())
				log.EndTime("tokenManagerService.ParseJWT", startTime, err)
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}
			//unmarshal string to our dto object
			if err := json.Unmarshal([]byte(claimsJSON), &unparsedData); err != nil {
				log.Alert(err.Error())
				log.EndTime("tokenManagerService.ParseJWT", startTime, err)
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}

		} else {
			err = errors.New("invalid token signature")
			log.Alert(err.Error())
			log.EndTime("tokenManagerService.ParseJWT", startTime, err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}

	}
	if err != nil {
		log.Alert(err.Error())
		log.EndTime("tokenManagerService.ParseJWT", startTime, err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	log.EndTime("tokenManagerService.ParseJWT", startTime, err)
	span.SetStatus(codes.Ok, "success")

	return &unparsedData, nil

}

// ValidateDID takes a magic DID Token and verifies if it is valid
func (t *tokenManagerService) ValidateDID(ctx context.Context, didToken string) (dto.MagicDIDValidationResults, error) {

	results := dto.MagicDIDValidationResults{IsDIDValid: false}
	span, labels := common.GenerateSpan("memberInfoService.ValidateDID", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "memberInfoService.ValidateDID"))
	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "memberInfoService.ValidateDID"))

	tk, err := token.NewToken(didToken)
	if err != nil {
		return results, err
	}

	err = tk.Validate(magicClient.ClientInfo.ClientId)
	if err != nil && err != token.ErrExpired && err != token.ErrNbfExpired {
		return results, err
	}

	pubAddr, err := tk.GetPublicAddress()
	if err != nil {
		return results, err
	}

	//if every above check is valid return that the did is valid along with the users wallet address
	results.IsDIDValid = true
	results.WalletAddress = pubAddr
	results.Issuer = tk.GetIssuer()

	log.EndTime("memberInfoService.ValidateDID", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return results, nil
}

func (t *tokenManagerService) ValidateTokenFormat(ctx context.Context, tokenFormate string) (dto.MagicDIDValidationResults, error) {
	span, labels := common.GenerateSpan("tokenManagerService.ValidateTokenFormat", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "tokenManagerService.ValidateTokenFormat"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "tokenManagerService.ValidateTokenFormat"))
	results := dto.MagicDIDValidationResults{IsAuthorized: false}
	jwtToken, err := t.ParseJWT(ctx, tokenFormate)
	if err != nil {
		return results, nil
	}

	if jwtToken.SUB == "signature_verification" && jwtToken.ISS == "community_page" && jwtToken.AUD == "FDA" {
		verify := t.VerifySignature(ctx, jwtToken.ADDR, jwtToken.SIG, jwtToken.Message)
		if verify {
			results.IsAuthorized = true

			log.EndTimeL(labels, "tokenManagerService.ValidateTokenFormat", startTime, nil)
			span.SetStatus(codes.Ok, "success")

			return results, nil
		}
	}

	expirationTime := time.Unix(int64(jwtToken.Exp), 0)
	// Public Address of the Wallet
	// change it to it
	if jwtToken.SUB == "fda_clgn_jwt" && jwtToken.AUD == "Public Address" && jwtToken.ISS == "FDA" && expirationTime.Before(time.Now()) {
		results.IsAuthorized = true

		log.EndTimeL(labels, "tokenManagerService.ValidateTokenFormat", startTime, nil)
		span.SetStatus(codes.Ok, "success")

		return results, nil
	}

	log.EndTimeL(labels, "tokenManagerService.ValidateTokenFormat", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return results, nil

}

func (t *tokenManagerService) VerifySignature(ctx context.Context, address string, signature string, message string) bool {
	// todo verify the message and public key with the private key (reverse process)
	span, labels := common.GenerateSpan("tokenManagerService.GenerateSignature", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "tokenManagerService.GenerateSignature"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "tokenManagerService.GenerateSignature"))
	// todo check the mili second for the time
	layout := "2006-01-02T15:04:05.000Z"
	parsedTime, err := time.Parse(layout, message)
	if err != nil {
		fmt.Println("Error parsing time:", err)
		log.EndTimeL(labels, "Error tokenManagerService.ValidateTokenFormat parsing ", startTime, nil)
		return false
	}

	if !parsedTime.After(time.Now().Add(10 * time.Minute)) {
		sigBytes := ethCommon.FromHex(strings.TrimPrefix(signature, "0x"))

		if len(sigBytes) != 65 {
			fmt.Println("Invalid signature length")
		}

		r, s := new(big.Int), new(big.Int)
		r.SetBytes(sigBytes[:32])
		s.SetBytes(sigBytes[32:64])
		v := new(big.Int).SetBytes([]byte{sigBytes[64]})

		if v.Cmp(big.NewInt(27)) < 0 {
			v.Add(v, big.NewInt(27))
		}
		hash := crypto.Keccak256Hash([]byte(address))

		sigPublicKey, err := crypto.Ecrecover(hash.Bytes(), sigBytes)
		if err != nil {
			fmt.Println("Error extracting public key:", err)
		}

		pubKeyECDSA, err := crypto.UnmarshalPubkey(sigPublicKey)
		if err != nil {
			fmt.Println("Error extracting public key:", err)
		}

		publicKeyBytes := crypto.FromECDSAPub(pubKeyECDSA)

		matches := bytes.Equal(sigPublicKey, publicKeyBytes)
		fmt.Println(matches)

		signatureNoRecoverID := sigBytes[:len(sigBytes)-1] // remove recovery id
		verified := crypto.VerifySignature(publicKeyBytes, hash.Bytes(), signatureNoRecoverID)

		log.EndTimeL(labels, "tokenManagerService.ParseJWT", startTime, nil)
		span.SetStatus(codes.Ok, "success")
		return verified
	}
	return false
}


func (t *tokenManagerService) VerifySignature(ctx context.Context, address string, signature string, message string) bool {
	// todo verify the message and public key with the private key (reverse process)
	span, labels := common.GenerateSpan("tokenManagerService.GenerateSignature", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "tokenManagerService.GenerateSignature"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "tokenManagerService.GenerateSignature"))
	// todo check the mili second for the time
	layout := "2006-01-02T15:04:05.000Z"
	m, _ := t.ExtractTimeStampFromMessage(ctx, message)
	parsedTime, err := time.Parse(layout, m)
	if err != nil {
		fmt.Println("Error parsing time:", err)
		log.EndTimeL(labels, "Error tokenManagerService.ValidateTokenFormat parsing ", startTime, nil)
		return false
	}

	if !parsedTime.After(time.Now().Add(10 * time.Minute)) {
		sigBytes := ethCommon.FromHex(strings.TrimPrefix(signature, "0x"))

		if len(sigBytes) != 65 {
			fmt.Println("Invalid signature length")
		}

		r, s := new(big.Int), new(big.Int)
		r.SetBytes(sigBytes[:32])
		s.SetBytes(sigBytes[32:64])
		v := new(big.Int).SetBytes([]byte{sigBytes[64]})

		if v.Cmp(big.NewInt(27)) < 0 {
			v.Add(v, big.NewInt(27))
		}
		hash := crypto.Keccak256Hash([]byte(address))

		sigPublicKey, err := crypto.Ecrecover(hash.Bytes(), sigBytes)
		if err != nil {
			fmt.Println("Error extracting public key:", err)
		}

		pubKeyECDSA, err := crypto.UnmarshalPubkey([]byte(t.signaturePublicKey))
		if err != nil {
			fmt.Println("Error extracting public key:", err)
		}

		publicKeyBytes := crypto.FromECDSAPub(pubKeyECDSA)

		matches := bytes.Equal(sigPublicKey, publicKeyBytes)
		fmt.Println(matches)

		signatureNoRecoverID := sigBytes[:len(sigBytes)-1] // remove recovery id
		verified := crypto.VerifySignature(publicKeyBytes, hash.Bytes(), signatureNoRecoverID)

		log.EndTimeL(labels, "tokenManagerService.ParseJWT", startTime, nil)
		span.SetStatus(codes.Ok, "success")
		return verified
	}
	return false
}

func (t *tokenManagerService) ExtractTimeStampFromMessage(ctx context.Context, message string) (string, error) {
	span, labels := common.GenerateSpan("tokenManagerService.GenerateSignature", ctx)
	defer span.End()
	span.AddEvent(fmt.Sprintf("Starting %s", "tokenManagerService.GenerateSignature"))

	startTime := log.StartTimeL(labels, fmt.Sprintf("Starting %s", "tokenManagerService.GenerateSignature"))
	timestampPattern := `Timestamp:(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)`
	regex := regexp.MustCompile(timestampPattern)

	// Find the first match of the pattern in the message
	matches := regex.FindStringSubmatch(message)
	if len(matches) < 2 {
		fmt.Println("No timestamp found")
		return "", errors.New("not timestamp found")
	}

	// Extract the timestamp string
	timestampString := matches[1]
	log.EndTimeL(labels, "tokenManagerService.ParseJWT", startTime, nil)
	span.SetStatus(codes.Ok, "success")
	return timestampString, nil
}