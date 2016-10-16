package camcloud

func checkSUAuth(suid string) bool {
	if suid == conf.Suid {
		return true
	} else {
		return false
	}
}