import { createConnection, Connection } from "typeorm";
import { Command, command, metadata, option, Options } from "clime";
import { pick, without, values } from "ramda";
import { createLogger, format, transports } from "winston";

const { info, error, warn } = createLogger({
    level: 'info',
    transports: [
        new transports.Console({ format: format.combine(format.colorize(), format.simple()) })
    ],
});

export class ConnectionOptions extends Options {
    @option({ required: true, flag: "h" }) public host: string;
    @option({ required: true, flag: "P" }) public password: string;
    @option({ required: true, flag: "u" }) public username: string;
    @option({ required: true, flag: "d" }) public database: string;
    @option({ required: true, flag: "f" }) public filename: string;
    @option({ flag: "p" }) public port: number;
}

async function connectMysql(config: Partial<ConnectionOptions>) {
    info("Connecting to mysql...");
    try {
        return await createConnection({
            ...config,
            name: "mysql",
            type: "mysql",
            // logging: true,
        });
    } catch (err) {
        error("Unable to connect.", err);
    }
}

async function connectSqlite({ filename }: Partial<ConnectionOptions>) {
    info("Connecting to sqlite...");
    try {
        return await createConnection({
            name: "sqlite",
            type: "sqlite",
            // logging: true,
            database: filename,
        });
    } catch (err) {
        error("Unable to connect.", err);
    }
}

@command({ description: "Migrate mysql to sqlite3." })
export default class extends Command {
    private mysql: Connection;
    private sqlite: Connection;

    private async migrateAcl() {
        const acls = await this.mysql.query("select * from acl");
        await this.sqlite.query(`delete from acl`);
        await Promise.all(acls.map(async acl => {
            await this.sqlite.query(`insert into acl(server_id, channel_id, priority, user_id, group_name, apply_here, apply_sub, grantpriv, revokepriv) values(?, ?, ?, ?, ?, ?, ?, ?, ?)`, [
                acl.server_id,
                acl.channel_id,
                acl.priority,
                acl.user_id,
                acl.group_name,
                acl.apply_here,
                acl.apply_sub,
                acl.grantpriv,
                acl.revokepriv,
            ]);
        }));
    }

    private async migrateBans() {
        const bans = await this.mysql.query("select * from bans");
        await this.sqlite.query(`delete from bans`);
        await Promise.all(bans.map(async ban => {
            await this.sqlite.query(`insert into bans(server_id, base, mask, name, hash, reason, start, duration) values(?, ?, ?, ?, ?, ?, ?, ?)`, [
                ban.server_id,
                ban.base,
                ban.mask,
                ban.name,
                ban.hash,
                ban.reason,
                ban.start,
                ban.duration,
            ]);
        }));
    }

    private async migrateChannelInfo() {
        const channelInfos = await this.mysql.query("select * from channel_info");
        await this.sqlite.query(`delete from channel_info`);
        await Promise.all(channelInfos.map(async channelInfo => {
            await this.sqlite.query(`insert into channel_info(server_id, channel_id, key, value) values(?, ?, ?, ?)`, [
                channelInfo.server_id,
                channelInfo.channel_id,
                channelInfo.key,
                channelInfo.value,
            ]);
        }));
    }

    private async migrateChannelLinks() {
        const channelLinks = await this.mysql.query("select * from channel_links");
        await this.sqlite.query(`delete from channel_links`);
        await Promise.all(channelLinks.map(async channelLinks => {
            await this.sqlite.query(`insert into channel_links(server_id, channel_id, link_id) values(?, ?, ?)`, [
                channelLinks.server_id,
                channelLinks.channel_id,
                channelLinks.link_id,
            ]);
        }));
    }

    private async migrateChannels() {
        const channels = await this.mysql.query("select * from channels");
        await this.sqlite.query(`delete from channels`);
        await Promise.all(channels.map(async channel => {
            info(`Migrating channel "${channel.name}`);
            await this.sqlite.query(`insert into channels(server_id, channel_id, parent_id, name, inheritacl) values(?, ?, ?, ?, ?)`, [
                channel.server_id,
                channel.channel_id,
                channel.parent_id,
                channel.name,
                channel.inheritacl,
            ]);
        }));
    }

    private async migrateConfig() {
        const configs = await this.mysql.query("select * from config");
        await this.sqlite.query(`delete from config`);
        await Promise.all(configs.map(async config => {
            await this.sqlite.query(`insert into config(server_id, key, value) values(?, ?, ?)`, [
                config.server_id,
                config.key,
                config.value,
            ]);
        }));
    }

    private async migrateGroupMembers() {
        const groupMemebers = await this.mysql.query("select * from group_members");
        await this.sqlite.query(`delete from group_members`);
        await Promise.all(groupMemebers.map(async groupMember => {
            await this.sqlite.query(`insert into group_members(group_id, server_id, user_id, addit) values(?, ?, ?, ?)`, [
                groupMember.group_id,
                groupMember.server_id,
                groupMember.user_id,
                groupMember.addit,
            ]);
        }));
    }

    private async migrateGroups() {
        const groups = await this.mysql.query("select * from groups");
        await this.sqlite.query(`delete from groups`);
        await Promise.all(groups.map(async group => {
            info(`Migrating group "${group.name}`);
            await this.sqlite.query(`insert into groups(group_id, server_id, name, channel_id, inherit, inheritable) values(?, ?, ?, ?, ?, ?)`, [
                group.group_id,
                group.server_id,
                group.name,
                group.channel_id,
                group.inherit,
                group.inheritable,
            ]);
        }));
    }

    private async migrateMeta() {
        const metas = await this.mysql.query("select * from meta");
        await this.sqlite.query(`delete from meta`);
        await Promise.all(metas.map(async meta => {
            await this.sqlite.query(`insert into meta(keystring, value) values(?, ?)`, [
                meta.keystring,
                meta.value,
            ]);
        }));
    }

    private async migrateServers() {
        const servers = await this.mysql.query("select * from servers");
        await this.sqlite.query(`delete from servers`);
        await Promise.all(servers.map(async server => {
            await this.sqlite.query(`insert into servers(server_id) values(?)`, [
                server.server_id,
            ]);
        }));
    }

    private async migrateSlog() {
        const slogs = await this.mysql.query("select * from slog");
        await this.sqlite.query(`delete from slog`);
        await Promise.all(slogs.map(async slog => {
            await this.sqlite.query(`insert into slog(server_id, msg, msgtime) values(?, ?, ?)`, [
                slog.server_id,
                slog.msg,
                slog.msgtime,
            ]);
        }));
    }

    private async migrateUserInfos() {
        const userInfos = await this.mysql.query("select * from user_info");
        await this.sqlite.query(`delete from user_info`);
        await Promise.all(userInfos.map(async userInfo => {
            await this.sqlite.query(`insert into user_info(server_id, user_id, key, value) values(?, ?, ?, ?)`, [
                userInfo.server_id,
                userInfo.user_id,
                userInfo.key,
                userInfo.value,
            ]);
        }));
    }

    private async migrateUsers() {
        const users = await this.mysql.query("select * from users");
        await this.sqlite.query(`delete from users`);
        await Promise.all(users.map(async user => {
            info(`Migrating user "${user.name}`);
            await this.sqlite.query(`insert into users(server_id, user_id, name, pw, salt, kdfiterations, lastchannel, texture, last_active) values(?, ?, ?, ?, ?, ?, ?, ?, ?)`, [
                user.server_id,
                user.user_id,
                user.name,
                user.pw,
                user.salt,
                user.kdfiterations,
                user.lastchannel,
                user.texture,
                user.last_active,
            ]);
        }));
    }

    private async checkTables() {
        const expectedTables = [
            "acl",
            "bans",
            "channel_info",
            "channel_links",
            "channels",
            "config",
            "group_members",
            "groups",
            "meta",
            "servers",
            "slog",
            "user_info",
            "users",
        ];
        try {
            const mysqlTables = await this.mysql.query(`show tables`);
            const sqliteTables = await this.sqlite.query(`select name from sqlite_master where type = 'table'`);
            const missingMysqlTables = without(mysqlTables.map(row => values(row)[0]), expectedTables);
            if (missingMysqlTables.length > 0) {
                error(`Mysql is missing the following tables: ${missingMysqlTables.join(", ")}`);
                return false;
            }
            const missingSqliteTables = without(sqliteTables.map(({ name }) => name), expectedTables);
            if (missingSqliteTables.length > 0) {
                error(`Sqlite is missing the following tables: ${missingSqliteTables.join(", ")}`);
                return false;
            }
        }
        catch (err) {
            error("Error determining exising tables", err);
            return false;
        }
        return true;
    }

    @metadata
    public async execute(config: ConnectionOptions) {
        this.mysql = await connectMysql(pick(["host", "password", "username", "database", "port"], config));
        this.sqlite = await connectSqlite(pick(["filename"], config));
        if (!this.mysql || !this.sqlite) {
            error("One database couldn't be connected. Terminating.");
            return;
        }
        if (!(await this.checkTables())) {
            error("Databases not ok. Terminating.");
        };
        try {
            info("migrating Servers...");
            await this.migrateServers();
            info("Done.");
        } catch (err) {
            error("Error.", err);
        }
        try {
            info("migrating Channels...");
            await this.migrateChannels();
            info("Done.");
        } catch (err) {
            error("Error.", err);
        }
        try {
            info("migrating Users...");
            await this.migrateUsers();
            info("Done.");
        } catch (err) {
            error("Error.", err);
        }
        try {
            info("migrating Groups...");
            await this.migrateGroups();
            info("Done.");
        } catch (err) {
            error("Error.", err);
        }
        try {
            info("migrating Acl...");
            await this.migrateAcl();
            info("Done.");
        } catch (err) {
            error("Error.", err);
        }
        try {
            info("migrating Bans...");
            await this.migrateBans();
            info("Done.");
        } catch (err) {
            error("Error.", err);
        }
        try {
            info("migrating ChannelInfo...");
            await this.migrateChannelInfo();
            info("Done.");
        } catch (err) {
            error("Error.", err);
        }
        try {
            info("migrating ChannelLinks...");
            await this.migrateChannelLinks();
            info("Done.");
        } catch (err) {
            error("Error.", err);
        }
        try {
            info("migrating Config...");
            await this.migrateConfig();
            info("Done.");
        } catch (err) {
            error("Error.", err);
        }
        try {
            info("migrating GroupMembers...");
            await this.migrateGroupMembers();
            info("Done.");
        } catch (err) {
            error("Error.", err);
        }
        try {
            info("migrating Meta...");
            await this.migrateMeta();
            info("Done.");
        } catch (err) {
            error("Error.", err);
        }
        try {
            info("migrating Slog...");
            await this.migrateSlog();
            info("Done.");
        } catch (err) {
            error("Error.", err);
        }
        try {
            info("migrating UserInfos...");
            await this.migrateUserInfos();
            info("Done.");
        } catch (err) {
            error("Error.", err);
        }
        info("Closing sqlite...");
        await this.sqlite.close();
        info("Closing mysql...");
        await this.mysql.close();
        info("Good bye");
    }
}
