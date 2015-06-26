/**
 * Copyright (c) 2012-2014 DataTorrent, Inc. All rights reserved.
 */
package com.datatorrent.stram.api;

/**
 * <p>Permission class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 2.0.0
 */
public enum Permission
{
  /**
   * View applications launched by others
   */
  VIEW_OTHER_USERS_APPS(true),
  /**
   * Manage applications launched by others
   */
  MANAGE_OTHER_USERS_APPS(true),
  /**
   * View global settings
   */
  VIEW_GLOBAL_CONFIG,
  /**
   * Edit global settings
   */
  EDIT_GLOBAL_CONFIG,
  /**
   * View other users' settings
   */
  VIEW_OTHER_USERS_CONFIG(true),
  /**
   * Edit other users' settings
   */
  EDIT_OTHER_USERS_CONFIG(true),
  /**
   * Access proxy to RM
   */
  ACCESS_RM_PROXY(true),
  /**
   * Launch App Packages
   */
  LAUNCH_APPS,
  /**
   * Upload Dashboards
   */
  UPLOAD_DASHBOARDS,
  /**
   * View other users' dashboards
   */
  VIEW_OTHER_USERS_DASHBOARDS(true),
  /**
   * Manage other users' dashboards
   */
  MANAGE_OTHER_USERS_DASHBOARDS(true),
  /**
   * Upload App Packages
   */
  UPLOAD_APP_PACKAGES,
  /**
   * View other users' App Packages
   */
  VIEW_OTHER_USERS_APP_PACKAGES(true),
  /**
   * Manage other users' App Packages
   */
  MANAGE_OTHER_USERS_APP_PACKAGES(true),
  /**
   * Upload Config Packages
   */
  UPLOAD_CONFIG_PACKAGES,
  /**
   * View other users' Config Packages
   */
  VIEW_OTHER_USERS_CONFIG_PACKAGES(true),
  /**
   * Manage other users' Config Packages
   */
  MANAGE_OTHER_USERS_CONFIG_PACKAGES(true),
  /**
   * Manage users (create/delete users, change password)
   */
  MANAGE_USERS(true),
  /**
   * Manage roles (create/delete roles)
   */
  MANAGE_ROLES(true),
  /**
   * View system alerts
   */
  VIEW_SYSTEM_ALERTS,
  /**
   * Manage system alerts
   */
  MANAGE_SYSTEM_ALERTS,
  /**
   * View system applications
   */
  VIEW_SYSTEM_APPS(true),
  /**
   * Manage system applications
   */
  MANAGE_SYSTEM_APPS(true);

  private boolean admin = false;

  Permission()
  {
  }

  Permission(boolean admin)
  {
    this.admin = admin;
  }

  public boolean isAdmin()
  {
    return admin;
  }

}
