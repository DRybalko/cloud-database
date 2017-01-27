package app_kvClient;

import java.util.HashMap;
import java.util.Map;

/**
 * This class contains all permission levels of the system. By default it supports three permission level:
 * admin, trainer, end-user. Each user is assigned to numeric permission level. Admin corresponds to level 3, 
 * trainer to level 2, etc.
 *
 */
public class PermissionController {

	private Map<String, Integer> permissions;
	
	public PermissionController() {
		this.permissions = new HashMap<>();
		addDefaultPermissions();
	}
	
	/*
	 * Sets default users/permissions for the system. This method should be extended to add new users to the system.
	 */
	private void addDefaultPermissions() {
		permissions.put("admin", 3);
		permissions.put("trainer", 2);
		permissions.put("end-user", 1);
	}
	
	public int getPermissionLevel(String role) {
		String roleKey = role.trim().toLowerCase();
		if (permissions.containsKey(roleKey)) return permissions.get(roleKey);
		else return -1;
	}
}
