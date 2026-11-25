import { ReactComponent as ApiIcon } from '@/assets/picture/api.svg';
import { ReactComponent as ModelIcon } from '@/assets/picture/model.svg';
import { ReactComponent as PasswordIcon } from '@/assets/picture/password.svg';
import { ReactComponent as ProfileIcon } from '@/assets/picture/profile.svg';
import { ReactComponent as TeamIcon } from '@/assets/picture/team.svg';
// import { ReactComponent as LogoutIcon } from '@/assets/svg/logout.svg';
import { UserSettingRouteKey } from '@/constants/setting';
// import { MonitorOutlined } from '@ant-design/icons';
import { ReactComponent as Monitor } from '@/assets/picture/monitor.svg';
import { ReactComponent as Register } from '@/assets/picture/register.svg';
import { SettingOutlined } from '@ant-design/icons';

export const UserSettingIconMap = {
  [UserSettingRouteKey.Profile]: <ProfileIcon />,
  [UserSettingRouteKey.Password]: <PasswordIcon />,
  [UserSettingRouteKey.Model]: <ModelIcon />,
  [UserSettingRouteKey.System]: <Monitor />,
  [UserSettingRouteKey.Team]: <TeamIcon />,
  // [UserSettingRouteKey.Logout]: <LogoutIcon />,
  [UserSettingRouteKey.Api]: <ApiIcon />,
  [UserSettingRouteKey.Register]: <Register />,
  [UserSettingRouteKey.Mcp]: <SettingOutlined />,
};

export * from '@/constants/setting';

export const LocalLlmFactories = [
  'Ollama',
  'Xinference',
  'LocalAI',
  'LM-Studio',
  'OpenAI-API-Compatible',
  'TogetherAI',
  'Replicate',
  'OpenRouter',
  'HuggingFace',
];

export enum TenantRole {
  Owner = 'owner',
  Invite = 'invite',
  Normal = 'normal',
}
