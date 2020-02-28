-- | These are types for Scheduled Trigger definition; see "Hasura.Eventing.ScheduledTrigger"
module Hasura.RQL.Types.ScheduledTrigger
  ( ScheduleType(..)
  , ScheduledTriggerName(..)
  , ScheduledEventId(..)
  , CreateScheduledTrigger(..)
  , CreateScheduledEvent(..)
  , RetryConfST(..)
  , formatTime'
  , defaultRetryConfST
  ) where

import           Data.Time.Clock
import           Data.Time.Clock.Units
import           Data.Time.Format
import           Data.Aeson
import           Data.Aeson.Casing
import           Data.Aeson.TH
import           Data.Char
import           Hasura.Prelude
import           System.Cron.Types
import           Hasura.Incremental
import           Data.Time.LocalTime (TimeZone(..),minutesToTimeZone)

import qualified Data.Text                     as T
import qualified Data.Aeson                    as J
import qualified Hasura.RQL.Types.EventTrigger as ET
import qualified Database.PG.Query             as Q

data RetryConfST
  = RetryConfST
  { rcstNumRetries  :: !Int
  , rcstIntervalSec :: !DiffTime
  , rcstTimeoutSec  :: !DiffTime
  , rcstTolerance   :: !NominalDiffTime
  } deriving (Show, Eq, Generic)

instance NFData RetryConfST
instance Cacheable RetryConfST

$(deriveJSON (aesonDrop 2 snakeCase){omitNothingFields=True} ''RetryConfST)

defaultRetryConfST :: RetryConfST
defaultRetryConfST =
  RetryConfST
  { rcstNumRetries = 0
  , rcstIntervalSec = seconds 10
  , rcstTimeoutSec = seconds 60
  , rcstTolerance = 21600 -- 6 hours
  }

data ScheduleType = Cron CronSchedule (Maybe TimeZone) | AdHoc (Maybe UTCTime)
  deriving (Show, Eq, Generic)

instance NFData ScheduleType
instance Cacheable ScheduleType

instance FromJSON ScheduleType where
  parseJSON =
    withObject "ScheduleType" $ \o -> do
      type' <- o .: "type"
      case type' of
        String "cron" -> Cron <$> o .: "value" <*> o .:? "utc-offset"
        String "adhoc" -> AdHoc <$> o .:? "value"
        _ -> fail "expected type to be cron or adhoc"

instance ToJSON ScheduleType where
  toJSON (Cron cs (Just offset)) = object ["type" .= String "cron", "value" .= toJSON cs, "utc-offset" .= (show offset)]
  toJSON (Cron cs Nothing) = object ["type" .= String "cron", "value" .= toJSON cs]
  toJSON (AdHoc (Just ts)) = object ["type" .= String "adhoc", "value" .= toJSON ts]
  toJSON (AdHoc Nothing) = object ["type" .= String "adhoc"]

-- convertUTCOffsetToTimeZone can take an offset in any one of
-- the following formats:
-- HHMM,HH:MM,(+/-)HHMM
-- If the length of the offset is 4, then it's assumed that it's a
-- positive offset.
convertUTCOffsetToTimeZone :: String -> Either String TimeZone
convertUTCOffsetToTimeZone offset
  | length offset == 4 = convertUTCOffsetToTimeZone ('+':offset)
convertUTCOffsetToTimeZone (h1:h2:':':m1:m2:"") =
  convertUTCOffsetToTimeZone('+':h1:h2:m1:m2:"")
convertUTCOffsetToTimeZone (p:h1:h2:':':m1:m2:"") =
  convertUTCOffsetToTimeZone(p:h1:h2:m1:m2:"")
convertUTCOffsetToTimeZone ('+':h1:h2:m1:m2:"")
  | and [(isDigit h1),(isDigit h2),(isDigit m1),(isDigit m2)] =
  let mins = (10 * (digitToInt h1) + (digitToInt h2)) * 60
             + (10 * (digitToInt m1) + (digitToInt m2))
  in Right $ TimeZone mins False ('+':h1:h2:m1:m2:"")
  | otherwise = Left "Invalid TimeZone Format"
convertUTCOffsetToTimeZone ('-':h1:h2:m1:m2:"") =
  case convertUTCOffsetToTimeZone ('+':h1:h2:m1:m2:"") of
    Left msg -> Left msg
    Right (TimeZone mins isSummerOnly offset) -> Right (TimeZone (-1 * mins) isSummerOnly offset)
convertUTCOffsetToTimeZone _ = Left "Invalid TimeZone Format"

instance FromJSON TimeZone where
  parseJSON = withText "TimeZone" $ \o ->
    either fail pure $ convertUTCOffsetToTimeZone $ T.unpack o

instance ToJSON TimeZone where
  toJSON (TimeZone _ _ offset) = String . T.pack $ offset

data CreateScheduledTrigger
  = CreateScheduledTrigger
  { stName           :: !ET.TriggerName
  , stWebhook        :: !ET.WebhookConf
  , stSchedule       :: !ScheduleType
  , stPayload        :: !(Maybe J.Value)
  , stRetryConf      :: !RetryConfST
  , stHeaders        :: ![ET.HeaderConf]
  } deriving (Show, Eq, Generic)

instance NFData CreateScheduledTrigger
instance Cacheable CreateScheduledTrigger

instance FromJSON CreateScheduledTrigger where
  parseJSON =
    withObject "CreateScheduledTrigger" $ \o -> do
      stName <- o .: "name"
      stWebhook <- o .: "webhook"
      stPayload <- o .:? "payload"
      stSchedule <- o .: "schedule"
      stRetryConf <- o .:? "retry_conf" .!= defaultRetryConfST
      stHeaders <- o .:? "headers" .!= []

      pure CreateScheduledTrigger {..}

$(deriveToJSON (aesonDrop 2 snakeCase){omitNothingFields=True} ''CreateScheduledTrigger)

data CreateScheduledEvent
  = CreateScheduledEvent
  { steName          :: !ET.TriggerName
  , steTimestamp     :: !UTCTime
  , stePayload       :: !(Maybe J.Value)
  } deriving (Show, Eq, Generic)

$(deriveJSON (aesonDrop 3 snakeCase){omitNothingFields=True} ''CreateScheduledEvent)

newtype ScheduledTriggerName
  = ScheduledTriggerName { unName :: ET.TriggerName }
  deriving (Show, Eq)

$(deriveJSON (aesonDrop 2 snakeCase) ''ScheduledTriggerName)

newtype ScheduledEventId
  = ScheduledEventId{ unEventId:: ET.EventId}
  deriving (Show, Eq)

$(deriveJSON (aesonDrop 2 snakeCase) ''ScheduledEventId)


-- Supported time string formats for the API:
-- (see FromJSON for ZonedTime: https://hackage.haskell.org/package/aeson-1.4.6.0/docs/src/Data.Aeson.Types.FromJSON.html#line-2050)

-- YYYY-MM-DD HH:MM Z YYYY-MM-DD HH:MM:SS Z YYYY-MM-DD HH:MM:SS.SSS Z

-- The first space may instead be a T, and the second space is optional. The Z represents UTC.
-- The Z may be replaced with a time zone offset of the form +0000 or -08:00,
-- where the first two digits are hours, the : is optional and the second two digits (also optional) are minutes.
formatTime' :: UTCTime -> T.Text
formatTime'= T.pack . formatTime defaultTimeLocale "%Y-%m-%d %H:%M:%S Z"
