--
-- Validate yb_db_admin can reassign object ownership
--
CREATE ROLE regress_user1;
CREATE ROLE regress_user2;
CREATE ROLE regress_user3;
GRANT regress_user1 TO regress_user3;
GRANT regress_user2 TO regress_user3;
SET SESSION AUTHORIZATION yb_db_admin;
REASSIGN OWNED BY regress_user1 TO regress_user2;
REASSIGN OWNED BY regress_user2 TO regress_user1;
-- should fail, database systems
REASSIGN OWNED BY postgres TO yb_db_admin;
-- should fail, user needs privileges on both old and new role
SET SESSION AUTHORIZATION regress_user1;
REASSIGN OWNED BY regress_user1 TO regress_user2;
REASSIGN OWNED BY regress_user2 TO regress_user1;
-- should succeed, user has privileges on both old and new role
SET SESSION AUTHORIZATION regress_user3;
REASSIGN OWNED BY regress_user1 TO regress_user2;
REASSIGN OWNED BY regress_user2 TO regress_user1;
