// helper -redis
// Redis related keys defined here
package helper

import "fmt"

const (
	PROJECT_ID = "QJCG"
)

func R_SessionIdKey(tid string) string {
	return fmt.Sprintf("%s_terminal_session_id:%s", PROJECT_ID, tid)
}

func R_SessionKey(sessionId string) string  {
	return fmt.Sprintf("%s_terminal_session:%s", PROJECT_ID, sessionId)
}

func R_OnlineKey(tid string) string  {
	return fmt.Sprintf("%s_online:%s", PROJECT_ID, tid)
}

func R_UpdatedKey(tid string) string  {
	return fmt.Sprintf("%s_terminal_has_update:%s", PROJECT_ID, tid)
}

func R_TermLastPVTKey(tid string) string  {
	return fmt.Sprintf("%s_terminal_last_pvt_key:%s", PROJECT_ID, tid)
}

func R_CarLastPVTKey(carId string) string  {
	return fmt.Sprintf("%s_car_last_pvt_key:%s", PROJECT_ID, carId)
}

func R_GPSTimeSetKey(tid string) string  {
	return fmt.Sprintf("%s_term_gps_time_set_key:%s", PROJECT_ID, tid)
}

func R_LongStopKey(tid string) string {
	return fmt.Sprintf("event:long_stop:%s", tid)
}

func R_LongStopPushedKey(tid string) string{
	return fmt.Sprintf("event:long_stop_pushed:%s", tid)
}

func R_OverspeedPushedKey(tid string) string {
	return fmt.Sprintf("event:over_speed_pushed:%s", tid)
}

func R_WarningSettingsKey(cid string) string {
	return fmt.Sprintf("%s:warn_option:%s", PROJECT_ID, cid)
}

func R_211UpdatingKey(tid string) string {
	return fmt.Sprintf("%s:_u9_time:%s", PROJECT_ID, tid)
}

func R_TermCacheKey(tid string) string {
	return fmt.Sprintf("%s_terminal_info_cache:%s", PROJECT_ID, tid)
}

func R_PushableMobileKey(mobile string) string {
	return fmt.Sprintf("%s_pushable_mobile:%s", PROJECT_ID, mobile)
}

func R_CorpCacheHashKey(cid string) string  {
	return fmt.Sprintf("%s_corp_info:%s", PROJECT_ID, cid)
}

func R_CorpNameShowKey(cid string) string  {
	return fmt.Sprintf("%s_corp_name_show:%s", PROJECT_ID, cid)
}

